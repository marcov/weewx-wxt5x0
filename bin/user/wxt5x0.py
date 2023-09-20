#!/usr/bin/env python
# Copyright 2017 Matthew Wall, all rights reserved
"""
Collect data from Vaisala WXT510 or WXT520 station.

Thanks to Antonis Katsonis for providing a Vaisala WXT520 for development.

http://www.vaisala.com/Vaisala%20Documents/User%20Guides%20and%20Quick%20Ref%20Guides/M210906EN-C.pdf

The WXT520 is available with the following serial communications:
 - RS232: ASCII automatic and polled; NMEA0183 v3; SDI12 v1.3
 - RS485: ASCII automatic and polled; NMEA0183 v3; SDI12 v1.3
 - RS422: ASCII automatic and polled; NMEA0183 v3; SDI12 v1.3
 - SDI12: v1.3 and v1.3 continuous

This driver supports only ASCII communications protocol.

The precipitation sensor measures both rain and hail.  Rain is measured as a
length, e.g., mm or inch (so the area is implied).  Hail is measured as hits
per area, e.g., hits per square cm or hits per square inch.

The precipitation sensor has three modes: precipitation on/off, tipping bucket,
and time based.  In precipitation on/off, the transmitter ends a precipitation
message 10 seconds after the first recognition of precipitation.  Rain duration
increases in 10 second steps.  Precipitation has ended when Ri=0.  This mode is
used for indication of the start and the end of precipitation.

In tipping bucket, the transmitter sends a precipitation message at each unit
increment (0.1mm/0.01 in).  This simulates conventional tipping bucket method.

In time based mode, the transmitter sends a precipitation message in the
intervals defined in the [I] field.  However, in polled protocols the autosend
mode tipping bucket should not be used as in it the resolution of the output
is decreased (quantized to tipping bucket tips).

The precipitation sensor can also operate in polled mode - it sends a precip
message when requested.

The rain counter reset can be manual, automatic, immediate, or limited.

The supervisor message controls error messaging and heater.
"""

# FIXME: test with and without error messages
# FIXME: test with and without crc

# FIXME: need to fix units of introduced observations:
#  rain_total
#  rain_duration
#  rain_intensity_peak
#  hail_duration
#  hail_intensity_peak
# these do not get converted, so LOOP and REC contain mixed units!
# also, REC does not know that rain_total is cumulative, not delta
# note that 'hail' (hits/area) is not cumulative like 'rain_total' (length)

from __future__ import with_statement
import time
import socket
import datetime
import pprint

import weewx.drivers

try:
    # New-style weewx logging
    #import weeutil.logger
    import logging

    log = logging.getLogger(__name__)

    def logdbg(msg):
        log.debug(msg)

    def loginf(msg):
        log.info(msg)

    def logerr(msg):
        log.error(msg)

except ImportError:
    # Old-style weewx logging
    import syslog

    def logmsg(level, msg):
        syslog.syslog(level, "wxt5x0: %s" % msg)

    def logdbg(msg):
        logmsg(syslog.LOG_DEBUG, msg)

    def loginf(msg):
        logmsg(syslog.LOG_INFO, msg)

    def logerr(msg):
        logmsg(syslog.LOG_ERR, msg)


DRIVER_NAME = "WXT5x0"
DRIVER_VERSION = "0.7"

MPS_PER_KPH = 0.277778
MPS_PER_MPH = 0.44704
MPS_PER_KNOT = 0.514444
MBAR_PER_PASCAL = 0.01
MBAR_PER_BAR = 1000.0
MBAR_PER_MMHG = 1.33322387415
MBAR_PER_INHG = 33.8639
MM_PER_INCH = 25.4
CM2_PER_IN2 = 6.4516


def loader(config_dict, _):
    return WXT5x0Driver(**config_dict[DRIVER_NAME])


def confeditor_loader():
    return WXT5x0ConfigurationEditor()


def _fmt(byte_str):
    """This will format raw bytes into a string of space-delimited hex."""
    try:
        # Python 2
        return " ".join(["%0.2X" % ord(c) for c in byte_str])
    except TypeError:
        # Python 3
        return " ".join(["%.2X" % c for c in byte_str])


class Station(object):
    def __init__(self, interface, address):
        self.crc_prefix = None
        self.terminator = b""
        self.address = address
        self.timeout = 3  # seconds
        self.interface = interface

    def open(self):
        self.interface.open()
        pass

    def close(self):
        self.interface.close()
        pass

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *_):
        self.close()

    def send_cmd(self, cmd):
        cmd = b"%d%s%s" % (self.address, cmd, self.terminator)
        self.interface.write(cmd)

    def get_data(self, cmd):
        #        if self.crc_prefix:
        #            cmd = cmd.replace('R', 'r')
        #            cmd = "%sxxx" % self.crc_prefix
        self.send_cmd(cmd)
        line = self.interface.readline()
        if line:
            line.replace(b"\x00", b"")  # eliminate any NULL characters
        return line

    def get_address(self):
        self.interface.write(b"?%s" % self.terminator)
        return self.interface.readline()

    def set_address(self, addr):
        self.send_cmd(b"A%d" % addr)

    def get_ack(self):
        return self.get_data(b"")

    def reset(self):
        self.send_cmd(b"XZ")

    def precip_counter_reset(self):
        self.send_cmd(b"XZRU")

    def precip_intensity_reset(self):
        self.send_cmd(b"XZRI")

    def measurement_reset(self):
        self.send_cmd(b"XZM")

    def set_automatic_mode(self):
        self.send_cmd(b"XU,M=R")

    def set_polled_mode(self):
        self.send_cmd(b"XU,M=P")

    def get_wind(self):
        return self.get_data(b"R1")

    def get_pth(self):
        return self.get_data(b"R2")

    def get_precip(self):
        return self.get_data(b"R3")

    def get_supervisor(self):
        return self.get_data(b"R5")

    def get_composite(self):
        return self.get_data(b"R0")

    @staticmethod
    def calc_crc(txt):
        # We need something that returns integers when iterated over.
        try:
            # Python 2
            byte_iter = [ord(x) for x in txt]
        except TypeError:
            # Python 3
            byte_iter = txt

        crc = 0
        for x in byte_iter:
            crc |= x
            for _ in range(1, 9):
                if crc << 16 == 1:
                    crc >>= 1
                    crc |= 0xA001
                else:
                    crc >>= 1
        a = 0x40 | (crc >> 12)
        b = 0x40 | ((crc >> 6) & 0x3F)
        c = 0x40 | (crc & 0x3F)
        return a + b + c

    OBSERVATIONS = {
        # aR1: wind message
        b"Dn": "wind_dir_min",
        b"Dm": "wind_dir_avg",
        b"Dx": "wind_dir_max",
        b"Sn": "wind_speed_min",
        b"Sm": "wind_speed_avg",
        b"Sx": "wind_speed_max",
        # aR2: pressure, temperature, humidity message
        b"Ta": "temperature",
        b"Ua": "humidity",
        b"Pa": "pressure",
        # aR3: precipitation message
        b"Rc": "rain",
        b"Rd": "rain_duration",
        b"Ri": "rain_intensity",
        b"Hc": "hail",
        b"Hd": "hail_duration",
        b"Hi": "hail_intensity",
        b"Rp": "rain_intensity_peak",
        b"Hp": "hail_intensity_peak",
        # dR5: supervisor message
        b"Th": "heating_temperature",
        b"Vh": "heating_voltage",
        b"Vs": "supply_voltage",
        b"Vr": "reference_voltage",
        b"Id": "information",
    }

    @staticmethod
    def parse(raw):
        # 0R0,Dn=000#,Dm=106#,Dx=182#,Sn=1.1#,Sm=4.0#,Sx=6.6#,Ta=16.0C,Ua=50.0P,Pa=1018.1H,Rc=0.00M,Rd=0s,Ri=0.0M,Hc=0.0M,Hd=0s,Hi=0.0M,Rp=0.0M,Hp=0.0M,Th=15.6C,Vh=0.0N,Vs=15.2V,Vr=3.498V,Id=Ant
        # 0R0,Dm=051D,Sm=0.1M,Ta=27.9C,Ua=39.4P,Pa=1003.2H,Rc=0.00M,Th=28.1C,Vh=0.0N
        # here is an unexpected result: no value for Dn!
        # 0R1,Dn=0m=032D,Sm=0.1M,Ta=27.9C,Ua=39.4P,Pa=1003.2H,Rc=0.00M,Th=28.3C,Vh=0.0N

        parsed = dict()

        for part in raw.strip().split(b","):
            cnt = part.count(b"=")
            if cnt == 0:
                # skip the leading identifier 0R0/0R1
                continue
            elif cnt == 1:
                abbr, vstr = part.split(b"=")
                if abbr == b"Id":  # skip the information field
                    continue
                obs = Station.OBSERVATIONS.get(abbr)
                if obs:
                    value = None
                    unit = None
                    try:
                        # Get the last character as a byte-string
                        unit = vstr[-1:]
                        if unit != b"#":  # '#' indicates invalid data
                            value = float(vstr[:-1])
                            value = Station.convert(obs, value, unit)
                    except ValueError as e:
                        logerr("parse failed for %s (%s):%s" % (abbr, vstr, e))
                    parsed[obs] = value
                else:
                    logdbg("unknown sensor %s: %s" % (abbr, vstr))
            else:
                logdbg("skip observation: '%s'" % part)
        return parsed

    @staticmethod
    def convert(obs, value, unit):
        """Convert units
        obs: a string, such as 'heating_temperature'
        value: float
        unit: a one character long byte-string
        """
        # convert from the indicated units to the weewx METRICWX unit system
        if "temperature" in obs:
            # [T] temperature C=celsius F=fahrenheit
            if unit == b"C":
                pass  # already C
            elif unit == b"F":
                value = (value - 32.0) * 5.0 / 9.0
            else:
                loginf("unknown unit '%s' for %s" % (unit, obs))
        elif "wind_speed" in obs:
            # [U] speed M=m/s K=km/h S=mph N=knots
            if unit == b"M":
                pass  # already m/s
            elif unit == b"K":
                value *= MPS_PER_KPH
            elif unit == b"S":
                value *= MPS_PER_MPH
            elif unit == b"N":
                value *= MPS_PER_KNOT
            else:
                loginf("unknown unit '%s' for %s" % (unit, obs))
        elif "pressure" in obs:
            # [P] pressure H=hPa P=pascal B=bar M=mmHg I=inHg
            if unit == b"H":
                pass  # already hPa/mbar
            elif unit == b"P":
                value *= MBAR_PER_PASCAL
            elif unit == b"B":
                value *= MBAR_PER_BAR
            elif unit == b"M":
                value *= MBAR_PER_MMHG
            elif unit == b"I":
                value *= MBAR_PER_INHG
            else:
                loginf("unknown unit '%s' for %s" % (unit, obs))
        elif "rain" in obs:
            # rain: accumulation duration intensity intensity_peak
            # [U] precip M=(mm s mm/h) I=(in s in/h)
            if unit == b"M":
                pass  # already mm
            elif unit == b"I":
                if "duration" not in obs:
                    value *= MM_PER_INCH
            elif unit == b"s":
                pass  # already seconds
            else:
                loginf("unknown unit '%s' for %s" % (unit, obs))
        elif "hail" in obs:
            # hail: accumulation duration intensity intensity_peak
            # [S] hail M=(hits/cm^2 s hits/cm^2h) I=(hits/in^2 s hits/in^2h)
            #          H=hits
            if unit == b"M":
                pass  # already cm^2
            elif unit == b"I":
                if "duration" not in obs:
                    value *= CM2_PER_IN2
            elif unit == b"s":
                pass  # already seconds
            else:
                loginf("unknown unit '%s' for %s" % (unit, obs))
        return value


class Interface(object):
    def __init__(self):
        pass


class SerialInterface(Interface):
    def __init__(self, port, baudrate, timeout):
        super().__init__()
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout

    def open(self):
        import serial

        logdbg("open serial port %s" % self.port)
        self.serial = serial.Serial(self.port, self.baudrate, timeout=self.timeout)

    def close(self):
        self.serial.close()

    def write(self, payload: bytes):
        logdbg(f"write [{len(payload)}]")
        self.serial.write(payload)

    def readline(self):
        line = self.serial.readline()
        logdbg(f"readline [{len(line)}]")
        return line


class TcpInterface(Interface):
    def __init__(self, host, port, timeout):
        super().__init__()
        self.host = host
        self.port = port
        self.socket = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP
        )
        self.timeout = timeout
        self.socket.settimeout(self.timeout)
        self.socketfile = self.socket.makefile()

    def open(self):
        try:
            self.socket.connect((self.host, self.port))
        except Exception as e:
            logerr(f"Unable to connect to TCP host {self.host}:{self.port}: {e}")
            raise
        pass

    def write(self, payload: bytes):
        logdbg(f"write [{len(payload)}]")
        self.socket.sendall(payload)
        pass

    def readline(self):
        try:
            line = self.socketfile.readline()
            logdbg(f"readline [{len(line)}]")

            try:
                bline = bytes(line, encoding="utf-8")
            except UnicodeDecodeError as e:
                logerr(f"Failed to encode line {line} to bytes: {e}")
                bline= b""
            return bline

        except TimeoutError:
            logerr(f"readline timed out")
            return b""

    def close(self):
        self.socket.close()
        pass


class StationAscii(Station):
    # ASCII over RS232, RS485, and RS422 defaults to 19200, 8, N, 1
    DEFAULT_BAUD = 19200

    def __init__(self, interface, address):
        super(StationAscii, self).__init__(interface, address)
        self.terminator = b"\r\n"


class StationNMEA(Station):
    DEFAULT_BAUD = 4800

    def __init__(self, interface, address):
        super().__init__(interface, address)
        self.terminator = b"\r\n"
        raise NotImplementedError("NMEA support not implemented")


class StationSDI12(Station):
    # SDI12 defaults to 1200, 7, E, 1
    DEFAULT_BAUD = 1200

    def __init__(self, interface, address):
        super().__init__(interface, address)
        self.terminator = b"\r\n"
        raise NotImplementedError("SDI12 support not implemented")


class WXT5x0ConfigurationEditor(weewx.drivers.AbstractConfEditor):
    @property
    def default_stanza(self):
        return """
[WXT5x0]
    # This section is for Vaisala WXT5x0 stations

    # The station model such as WXT510 or WXT520
    model = WXT520

    # 'serial' or 'net'
    interface = net

    # The serial port to which the station is connected
    serial_port = /dev/ttyUSB0

    # host and port
    net_host = 127.0.0.1
    net_port = 2323

    # The communication protocol to use, one of ascii, nmea, or sdi12
    protocol = ascii

    # The device address
    address = 0

    # The driver to use
    driver = user.wxt5x0
"""

    def prompt_for_settings(self):
        print("Specify the model")
        model = self._prompt("model", "WXT520")

        print("Specify the interface ('serial' or 'net')")
        interface = self._prompt("interface", "serial", ["serial", "net"])

        print("Specify the protocol (ascii, nmea, or sdi12)")
        protocol = self._prompt("protocol", "ascii", ["ascii", "nmea", "sdi12"])

        print("Specify the serial port on which the station is connected, for")
        print("example /dev/ttyUSB0 or /dev/ttyS0.")
        serial_port = self._prompt("serial_port", "/dev/ttyUSB0")

        print("Specify the net host")
        net_host = self._prompt("net_host", "127.0.0.1")

        print("Specify the net port")
        net_port = self._prompt("net_port", "127.0.0.1")

        print("Specify the device address")
        address = self._prompt("address", 0)
        return {
            "model": model,
            "interface": interface,
            "serial_port": serial_port,
            "net_host": net_host,
            "net_port": net_port,
            "protocol": protocol,
            "address": address,
        }


class WXT5x0Driver(weewx.drivers.AbstractDevice):
    DEFAULT_PORT = "/dev/ttyUSB0"

    # map sensor names to schema names
    DEFAULT_MAP = {
        "windDir": "wind_dir_avg",
        "windSpeed": "wind_speed_avg",
        "windGustDir": "wind_dir_max",
        "windGust": "wind_speed_max",
        "outTemp": "temperature",
        "outHumidity": "humidity",
        "pressure": "pressure",
        "rain_total": "rain",
        "rainRate": "rain_intensity",
        "hail": "hail",
        "hailRate": "hail_intensity",
        "heatingTemp": "heating_temperature",
        "heatingVoltage": "heating_voltage",
        "supplyVoltage": "supply_voltage",
        "referenceVoltage": "reference_voltage",
    }

    def __init__(self, **stn_dict):
        loginf("driver version is %s" % DRIVER_VERSION)
        self._model = stn_dict.get("model", "WXT520")
        self._max_tries = int(stn_dict.get("max_tries", 5))
        self._retry_wait = int(stn_dict.get("retry_wait", 10))
        self._poll_interval = int(stn_dict.get("poll_interval", 10))
        self._sensor_map = dict(WXT5x0Driver.DEFAULT_MAP)
        self.last_rain_total = None

        protocol = stn_dict.get("protocol", "ascii").lower()

        if protocol == "ascii":
            sta_cls = StationAscii
        elif protocol == "nmea":
            sta_cls = StationNMEA
        elif protocol == "sdi12":
            sta_cls = StationSDI12
        else:
            raise RuntimeError(f"Not a valid protocol {protocol}")

        iface_name = stn_dict.get("interface", "serial")

        if iface_name == "ascii":
            baud = sta_cls.DEFAULT_BAUD
            baud = int(stn_dict.get("baud", baud))
            port = stn_dict.get("port", WXT5x0Driver.DEFAULT_PORT)
            interface = SerialInterface(port, baud, 3)
        elif iface_name == "net":
            host = stn_dict.get("net_host")
            port = stn_dict.get("net_port", 0)
            port = int(port)
            interface = TcpInterface(host, port, 10)
        else:
            raise RuntimeError(f"Not a valid interface {iface_name}")

        address = int(stn_dict.get("address", 0))
        self._station = sta_cls(interface, address)

        self._station.open()

    def closePort(self):
        self._station.close()

    @property
    def hardware_name(self):
        return self._model

    def genLoopPackets(self):
        self._station.reset()
        self._station.set_polled_mode()

        while True:
            for cnt in range(self._max_tries):
                try:
                    raw = self._station.get_composite()
                    logdbg("raw: %s" % _fmt(raw))
                    data = Station.parse(raw)
                    logdbg("parsed: %s" % data)
                    packet = self._data_to_packet(data)
                    logdbg("mapped: %s" % packet)
                    if packet:
                        yield packet
                    break
                except IOError as e:
                    logerr(
                        "Failed attempt %d of %d to read data: %s"
                        % (cnt + 1, self._max_tries, e)
                    )
                    logdbg("Waiting %d seconds" % self._retry_wait)
                    time.sleep(self._retry_wait)
            else:
                raise weewx.RetriesExceeded(
                    "Read failed after %d tries" % self._max_tries
                )
            if self._poll_interval:
                time.sleep(self._poll_interval)

    def _data_to_packet(self, data):
        # if there is a mapping to a schema name, use it.  otherwise use the
        # sensor naming native to the hardware.
        packet = dict()
        for name in data:
            obs = name
            for field in self._sensor_map:
                if self._sensor_map[field] == name:
                    obs = field
                    break
            packet[obs] = data[name]
        if packet:
            packet["dateTime"] = int(time.time() + 0.5)
            packet["usUnits"] = weewx.METRICWX
        if "rain_total" in packet:
            packet["rain"] = self._delta_rain(
                packet["rain_total"], self.last_rain_total
            )
            self.last_rain_total = packet["rain_total"]
        return packet

    @staticmethod
    def _delta_rain(rain, last_rain):
        if last_rain is None:
            loginf("skipping rain measurement of %s: no last rain" % rain)
            return None
        if rain < last_rain:
            loginf(
                "rain counter wraparound detected: new=%s last=%s" % (rain, last_rain)
            )
            return rain
        return rain - last_rain


# define a main entry point for basic testing of the station without weewx
# engine and service overhead.  invoke this as follows from the weewx root dir:
#
# PYTHONPATH=bin python bin/user/wxt5x0.py

if __name__ == "__main__":
    import optparse
    import syslog

    usage = """%prog [options] [--debug] [--help]"""
    syslog.openlog("wxt5x0", syslog.LOG_PID | syslog.LOG_CONS)
    syslog.setlogmask(syslog.LOG_UPTO(syslog.LOG_INFO))
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("--version", action="store_true", help="display driver version")
    parser.add_option(
        "--debug",
        action="store_true",
        help="display diagnostic information while running",
    )
    parser.add_option("--protocol", help="ascii, nmea, or sdi12", default="ascii")
    parser.add_option(
        "--serial_port",
        help="serial port to which the station is connected",
        default=WXT5x0Driver.DEFAULT_PORT,
    )
    parser.add_option("--baud", type=int, help="baud rate", default=19200)
    parser.add_option("--address", type=int, help="device address", default=0)
    parser.add_option(
        "--poll-interval",
        metavar="POLL",
        type=int,
        help="poll interval, in seconds",
        default=3,
    )
    parser.add_option("--get-wind", help="get a single wind message")
    parser.add_option("--get-pth", help="get a pressure/temperature/humidity message")
    parser.add_option("--get-precip", help="get a single precipitation message")
    parser.add_option("--get-supervisor", help="get a single supervisor message")
    parser.add_option("--get-composite", help="get a single composite message")
    parser.add_option("--test-crc", metavar="STRING", help="verify the CRC calculation")

    parser.add_option(
        "--interface", type=str, help="station interface", default="serial"
    )
    parser.add_option("--net_host", type=str, help="net_host", default="127.0.0.1")
    parser.add_option("--net_port", type=int, help="net port", default=2323)

    (options, args) = parser.parse_args()

    if options.version:
        print("%s driver version %s" % (DRIVER_NAME, DRIVER_VERSION))
        exit(1)

    if options.debug:
        syslog.setlogmask(syslog.LOG_UPTO(syslog.LOG_DEBUG))

    if options.test_crc:
        print("string: '%s'" % options.test_crc)
        print("crc: '%s'" % Station.calc_crc(options.test_crc))
        exit(0)

    if options.protocol == "ascii":
        sta_cls = StationAscii
    elif options.protocol == "nmea":
        sta_cls = StationNMEA
    elif options.protocol == "sdi12":
        sta_cls = StationSDI12
    else:
        print("unknown protocol '%s'" % options.protocol)
        exit(1)

    if options.interface == "serial":
        interface = SerialInterface(options.serial_port, options.baud, 3)
    elif options.interface == "net":
        interface = TcpInterface(options.net_host, options.net_port, 10)
    else:
        raise RuntimeError(f"Not a valid interface {options.interface}")

    with sta_cls(interface, options.address) as s:
        if options.get_wind:
            print("%s" % s.get_wind().strip())
        elif options.get_pth:
            print("%s" % s.get_pth().strip())
        elif options.get_precip:
            print("%s" % s.get_precip().strip())
        elif options.get_supervisor:
            print("%s" % s.get_supervisor().strip())
        elif options.get_composite:
            print("%s" % s.get_composite().strip())
        else:
            s.reset()
            s.set_polled_mode()
            while True:
                data = s.get_composite().strip()
                parsed = Station.parse(data)
                if parsed:
                    print(f"{datetime.datetime.now()}: {pprint.pformat(parsed)}")
                else:
                    print(f"{datetime.datetime.now()}: [no data]")
                time.sleep(options.poll_interval)

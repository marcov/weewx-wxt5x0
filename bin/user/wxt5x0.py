#!/usr/bin/env python
# Copyright 2023 Marco Vedovati
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

# FIXME: need to fix units of introduced observations:
#  rain_total
#  rain_duration
#  rain_intensity_peak
#  hail_duration
#  hail_intensity_peak
# these do not get converted, so LOOP and REC contain mixed units!
# also, REC does not know that rain_total is cumulative, not delta
# note that 'hail' (hits/area) is not cumulative like 'rain_total' (length)

from enum import Enum, auto
import logging
import pprint
import socket
import sys
import time
import threading

import weewx.drivers


log = logging.getLogger(__name__)


def logdbg(msg):
    log.debug(msg)


def loginfo(msg):
    log.info(msg)


def logwarn(msg):
    log.warning(msg)


def logerr(msg):
    log.error(msg)


DRIVER_NAME = "WXT5x0"
DRIVER_VERSION = "2.0"

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


def hexlify(byte_str):
    """This will format raw bytes into a string of space-delimited hex."""
    return " ".join(["%.2X" % c for c in byte_str])


class Station(object):
    MAX_RX_ERROR_RETRIES = 50
    TARGET_UNIT = weewx.US
    CRC_LEN = 3

    class BadAddress(Exception):
        def __init__(self, rcvd_address, *args):
            super().__init__(*args)
            self.rcvd_address = rcvd_address

        pass

    class MessageMode(Enum):
        Polled = auto()
        Auto = auto()

    def __init__(self, interface, address: int, use_crc: bool):
        self.use_crc = use_crc
        self.terminator = ""
        self.address = address
        self.interface: Interface = interface
        self.message_mode = None
        self.last_data_msg_time = None

        loginfo(
            f"Creating Station - use_crc: {use_crc}, address: {address}, interface: {interface}"
        )

    def setup(self):
        loginfo("Setting up station")
        self.interface.open()
        time.sleep(1)

        # Pause auto mode
        loginfo("Setting polled mode")
        self.set_polled_mode()

        # Flush buffer
        loginfo("Flushing interface")
        self.interface.flush(1)

        self.set_supervisor_config()

        loginfo("Resetting precipitation data")
        self.precip_counter_reset()
        self.precip_intensity_reset()

        self.setup_wind_sensor()
        self.setup_rain_sensor()
        self.setup_thp_sensors()

    def setup_wind_sensor(self):
        loginfo("Setting up wind sensor")

        # turn on average and max direction and speed
        self.send_and_receive("WU,R=0110110001101100")
        time.sleep(0.5)

        # set update and averaging interval.
        if Station.TARGET_UNIT == weewx.METRICWX:
            # Set to m/s to match METRICWX
            unit = "M"
        elif Station.TARGET_UNIT == weewx.US:
            unit = "S"
        else:
            raise RuntimeError(f"Bad station target unit: {self.TARGET_UNIT}")

        self.send_and_receive(f"WU,I=30,A=30,U={unit},D=0,F=4")
        time.sleep(0.5)

    def setup_rain_sensor(self):
        loginfo("Setting up rain sensor")

        # turn on rain/hail amount and intensity
        self.send_and_receive("RU,R=1111111111111111")
        time.sleep(0.5)

        # U: rain units
        # S: hail units
        #
        # M: autosend mode
        # - R: precipitation ON/OFF
        # - C: tipping bucket 0.1mm
        # - T: time interval based
        #
        # Z: reset mode
        # - A = auto
        # - M = manual via aXZRU
        if Station.TARGET_UNIT == weewx.METRICWX:
            # set to metric to match METRICWX (mm, mm/h)
            unit = "M"
        elif Station.TARGET_UNIT == weewx.US:
            unit = "I"
        else:
            raise RuntimeError(f"Bad station target unit: {self.TARGET_UNIT}")

        self.send_and_receive(f"RU,U={unit},S={unit},M=T,Z=M")

        time.sleep(0.5)

    def setup_thp_sensors(self):
        loginfo("Setting up temperature, humidity, pressure sensors")

        # turn on air pressure, temperature and humidity
        self.send_and_receive("TU,R=1111000011110000")

        # set pressure units to in/Hg, temperature units to Fahrenheit
        #
        # Nope, set P in hPA and T in C
        if Station.TARGET_UNIT == weewx.METRICWX:
            # set to metric to match METRICWX (mm, mm/h)
            p_unit = "H"
            t_unit = "C"
        elif Station.TARGET_UNIT == weewx.US:
            p_unit = "I"
            t_unit = "F"
        else:
            raise RuntimeError(f"Bad station target unit: {self.TARGET_UNIT}")
        self.send_and_receive(f"TU,P={p_unit},T={t_unit}")

    def set_supervisor_config(self):
        loginfo("Setting supervisor configuration")

        # turn on all info
        self.send_and_receive("SU,R=1111100011111000")
        time.sleep(0.5)

        # turn on error reporting
        self.send_and_receive(f"SU,I=15,S=Y,H=Y")
        time.sleep(0.5)

    def close(self):
        self.interface.close()
        pass

    def __enter__(self):
        self.setup()
        return self

    def __exit__(self, *_):
        self.close()

    def tx_command(self, command: str, prepend_address: bool = True):
        """Send a command.

        command (str): string without address and terminator.
        prepend_address (bool): if True, include the address in the TX.
        """

        if prepend_address:
            addr = self.address
        else:
            addr = ""

        # For CRC, the first command letter must be lowercase
        if self.use_crc:
            command = command[0].lower() + command[1:]

        if self.use_crc:
            crc = self.calc_crc(f"{addr}{command}")
        else:
            crc = ""

        packet = f"{addr}{command}{crc}{self.terminator}".encode(encoding="utf-8")
        self.interface.write(packet)

    def rx_response(self) -> str:
        eol = self.terminator.encode(encoding="utf-8")
        line = self.interface.readline(eol=eol).decode(encoding="utf-8")

        # Check address
        rcvd_address = line[0]

        if rcvd_address != self.address:
            raise self.BadAddress(
                rcvd_address,
                f"Received address {rcvd_address} does not match configured address {self.address} - line: '{line}'",
            )

        return line

    def send_and_receive(self, command: str | None = None) -> str:
        BAD_RESPONSES = ("Unknown cmd error", "Use chksum")
        "UtX,Sync/address errorJRM"

        # Retry only if we can re-TX the command
        RETRIES_COUNT = Station.MAX_RX_ERROR_RETRIES if command else 1
        ith_retry = 0
        try:
            for ith_retry in range(RETRIES_COUNT):
                if command:
                    if ith_retry > 0:
                        loginfo(f"Retrying TX ... [retry {ith_retry}/{RETRIES_COUNT}]")
                    self.tx_command(command)

                resp = self.rx_response()

                # We continue on any error, or break on success
                if not resp:
                    logerr("Response is empty")
                    continue

                if any(substr in resp for substr in BAD_RESPONSES):
                    logerr(f"Response is bad: '{resp}'")
                    continue

                # Check CRC if enabled.
                if self.use_crc:
                    if len(resp) <= Station.CRC_LEN:
                        logerr(f"Response too short to contain a CRC: '{resp}'")
                        continue

                    payload, received_crc = (
                        resp[: -Station.CRC_LEN],
                        resp[-Station.CRC_LEN :],
                    )
                    expected_crc = self.calc_crc(payload)
                    if received_crc != expected_crc:
                        logerr(
                            f"CRC check failed: expected '{expected_crc}' - received '{received_crc}'"
                        )
                        continue

                    logdbg(f"CRC check passed: '{received_crc}'")

                    # Strip CRC
                    resp = payload

                logdbg(f"Response received is good [retry {ith_retry}/{RETRIES_COUNT}]")
                break

            else:
                msg = f"send_and_receive failed [retry {ith_retry}/{RETRIES_COUNT}]"
                logerr(msg)
                raise RuntimeError(msg)

        except self.BadAddress as ex:
            logwarn(ex)
            self.set_address(ex.rcvd_address)
            resp = ""

        return resp

    def set_address(self, curr_address):
        loginfo(f"Setting address to {self.address}")
        self.tx_command(f"{curr_address}XU,A={self.address}", False)
        self.rx_response()

    def precip_counter_reset(self):
        self.send_and_receive("XZRU")

    def precip_intensity_reset(self):
        self.send_and_receive("XZRI")

    def measurement_reset(self):
        loginfo("measurement reset")
        self.send_and_receive("XZM")

    def set_automatic_mode(self):
        loginfo("set auto mode")
        self.send_and_receive(f"XU,A={self.address},M=A")
        self.message_mode = self.MessageMode.Auto

    def set_polled_mode(self):
        poll_mode = "p" if self.use_crc else "P"
        self.send_and_receive(f"XU,A={self.address},M={poll_mode}")
        self.message_mode = self.MessageMode.Polled

    def get_wind(self):
        return self.send_and_receive("R1")

    def get_pth(self):
        return self.send_and_receive("R2")

    def get_precip(self):
        return self.send_and_receive("R3")

    def get_supervisor(self):
        return self.send_and_receive("R5")

    def get_composite_data_message(self):
        return self.send_and_receive("R0")

    def get_data_message(self, poll_interval):
        if self.message_mode == self.MessageMode.Auto:
            data_msg = self.send_and_receive()
        else:
            assert poll_interval >= 1, f"Poll interval {poll_interval} is too small"
            if (
                self.last_data_msg_time
                and (time.time() - self.last_data_msg_time) < poll_interval
            ):
                time.sleep(poll_interval)
            data_msg = self.get_composite_data_message()

        if data_msg:
            self.last_data_msg_time = time.time()

        return data_msg

    @staticmethod
    def calc_crc(payload: str) -> str:
        def get_int_crc(payload: str) -> int:
            """Compute an unsigned int CRC."""
            crc = 0
            for b in payload:
                crc = crc ^ ord(b)
                for _ in range(8):
                    if crc & 0x01:
                        crc >>= 1
                        crc = crc ^ 0xA001
                    else:
                        crc >>= 1
                crc = crc & 0xFFFF
            return crc

        def to_ascii(crc: int) -> str:
            """Encode an unsigned int CRC to ASCII."""
            a = 0x40 | (crc >> 12)
            b = 0x40 | ((crc >> 6) & 0x3F)
            c = 0x40 | (crc & 0x3F)
            return chr(a) + chr(b) + chr(c)

        crc = get_int_crc(payload)
        as_ascii = to_ascii(crc)
        logdbg(f"[CRC] hex {hex(crc)} ASCII '{as_ascii}'")
        return as_ascii

    MEASURES = {
        # aR1: wind message
        "Dm": "wind_dir_avg",
        "Dn": "wind_dir_min",
        "Dx": "wind_dir_max",
        "Sm": "wind_speed_avg",
        "Sn": "wind_speed_min",
        "Sx": "wind_speed_max",
        # aR2: pressure, temperature, humidity message
        "Pa": "pressure",
        "Ta": "temperature",
        "Tp": "temperature_internal",
        "Ua": "humidity",
        # aR3: precipitation message
        "Hc": "hail",
        "Hd": "hail_duration",
        "Hi": "hail_intensity",
        "Hp": "hail_intensity_peak",
        "Rc": "rain_accumulation",
        "Rd": "rain_duration",
        "Ri": "rain_intensity",
        "Rp": "rain_intensity_peak",
        # dR5: supervisor message
        "Id": "information",
        "Th": "heating_temperature",
        "Vh": "heating_voltage",
        "Vr": "reference_voltage",
        "Vs": "supply_voltage",
    }

    @classmethod
    def parse(cls, raw_msg: str) -> dict:
        # 0R0,Dn=000#,Dm=106#,Dx=182#,Sn=1.1#,Sm=4.0#,Sx=6.6#,Ta=16.0C,Ua=50.0P,Pa=1018.1H,Rc=0.00M,Rd=0s,Ri=0.0M,Hc=0.0M,Hd=0s,Hi=0.0M,Rp=0.0M,Hp=0.0M,Th=15.6C,Vh=0.0N,Vs=15.2V,Vr=3.498V,Id=Ant
        # 0R0,Dm=051D,Sm=0.1M,Ta=27.9C,Ua=39.4P,Pa=1003.2H,Rc=0.00M,Th=28.1C,Vh=0.0N
        # here is an unexpected result: no value for Dn!
        # 0R1,Dn=0m=032D,Sm=0.1M,Ta=27.9C,Ua=39.4P,Pa=1003.2H,Rc=0.00M,Th=28.3C,Vh=0.0N

        parsed = dict()

        for part in raw_msg.strip().split(","):
            cnt = part.count("=")

            if cnt == 0:
                # skip the leading identifier 0R0/0R1
                continue

            elif cnt == 1:
                abbrev, value_unit = part.split("=")

                if abbrev == "Id":  # skip the information field
                    continue

                measure = Station.MEASURES.get(abbrev)
                if measure:
                    value = None
                    unit = None
                    try:
                        # Get the last character as a byte-string
                        unit = value_unit[-1:]
                        if unit != "#":  # '#' indicates invalid data
                            value = float(value_unit[:-1])
                            value = cls.check_units(measure, value, unit)
                        else:
                            logwarn(
                                f"Invalid data for measure={measure}: part={part} - abbrev={abbrev} value_unit={value_unit} unit={unit}"
                            )
                    except ValueError as e:
                        logerr(
                            f"parse failed for abbrev={abbrev} value_unit={value_unit} unit={unit} measure={measure} raw_msg={raw_msg}: {e}"
                        )

                    parsed[measure] = value

                else:
                    logwarn("unknown sensor %s: %s" % (abbrev, value_unit))

            else:
                logwarn("skip observation: '%s'" % part)

        return parsed

    @staticmethod
    def check_units(measure: str, value: float, unit: str):
        """Refer to: https://weewx.com/docs/customizing.htm#units"""
        if Station.TARGET_UNIT == weewx.US:
            return Station.check_units_us(measure, value, unit)
        elif Station.TARGET_UNIT == weewx.METRICWX:
            return Station.check_units_metricwx(measure, value, unit)
        else:
            raise RuntimeError(f"Bad station target unit: {Station.TARGET_UNIT}")

    @staticmethod
    def check_units_us(measure: str, value: float, unit: str):
        """Convert units
        measure: a string, such as 'heating_temperature'
        value: float
        unit: a one character long byte-string
        """

        assert len(unit) == 1, "Unit needs to be one char"

        #
        # From the docs:
        # The difference between METRICWX, and METRIC is that the former uses
        # mm instead of cm for rain, and m/s instead of km/hr for wind speed
        #
        # convert from the indicated units to the weewx METRICWX unit system
        if "temperature" in measure:
            assert unit == "F"
            return value

        elif measure.startswith("wind_speed"):
            assert unit == "S"
            return value

        elif measure.startswith("wind_dir"):
            assert unit == "D"
            return value

        elif measure.startswith("pressure"):
            assert unit == "I"
            return value

        elif measure.startswith("humidity"):
            assert unit == "P"
            return value

        elif measure.startswith("rain_intensity") or measure == "rain_accumulation":
            assert unit == "I"
            return value

        elif measure.startswith("hail_intensity") or measure == "hail":
            assert unit == "I"
            return value

        elif measure.endswith("duration"):
            assert unit == "s"
            return value

        elif measure == "heating_voltage":
            # N = heating option is available but have been disabled by user
            # or the heating temperature is over the high control limit.
            #
            # V = heating is on at 50% duty cycle and the heating temperature
            # is between the high and middle control limits.
            #
            # W = heating is on at 100% duty cycle and the heating temperature
            # is between the low and middle control limits.
            #
            # F = heating is on at 50% duty cycle and the heating temperature
            # is below the low control limit.
            assert unit in {"N", "V", "W", "F"}
            return value

        elif measure.endswith("voltage"):
            assert unit == "V"
            return value

        elif measure == "information":
            return value

        logerr(f"Cannot convert {measure} {value} {unit}")
        return None

    @staticmethod
    def check_units_metricwx(measure: str, value: float, unit: str):
        """Convert units
        measure: a string, such as 'heating_temperature'
        value: float
        unit: a one character long byte-string
        """

        assert len(unit) == 1, "Unit needs to be one char"

        #
        # From the docs:
        # The difference between METRICWX, and METRIC is that the former uses
        # mm instead of cm for rain, and m/s instead of km/hr for wind speed
        #
        # convert from the indicated units to the weewx METRICWX unit system
        if measure.startswith("temperature"):
            # [T] temperature C=celsius F=fahrenheit
            if unit == "C":
                pass  # already C
            elif unit == "F":
                value = 0
            else:
                logerr(f"Unknown unit {unit} for {measure}")

        elif measure.startswith("wind_speed"):
            # [U] speed M=m/s K=km/h S=mph N=knots
            if unit == "M":
                pass  # already m/s
            elif unit == "K":
                value *= MPS_PER_KPH
            elif unit == "S":
                value *= MPS_PER_MPH
            elif unit == "N":
                value *= MPS_PER_KNOT
            else:
                logerr(f"Unknown unit {unit} for {measure}")

        elif measure.startswith("wind_speed"):
            assert unit == "D"
            pass

        elif measure.startswith("pressure"):
            # [P] pressure H=hPa P=pascal B=bar M=mmHg I=inHg
            if unit == "H":
                pass  # already hPa/mbar
            elif unit == "P":
                value *= MBAR_PER_PASCAL
            elif unit == "B":
                value *= MBAR_PER_BAR
            elif unit == "M":
                value *= MBAR_PER_MMHG
            elif unit == "I":
                value *= MBAR_PER_INHG
            else:
                logerr(f"Unknown unit {unit} for {measure}")

        elif measure.startswith("rain"):
            # rain: accumulation duration intensity intensity_peak
            # [U] precip M=(mm s mm/h) I=(in s in/h)
            if unit == "M":
                pass  # already mm
            elif unit == "I":
                assert f"Unsupported rain unit {unit}"
            elif unit == "s":
                pass  # already seconds
            else:
                logerr(f"Unknown unit {unit} for {measure}")

        elif measure.startswith("hail"):
            # hail: accumulation duration intensity intensity_peak
            # [S] hail M=(hits/cm^2 s hits/cm^2h) I=(hits/in^2 s hits/in^2h)
            #          H=hits
            if unit == "M":
                pass  # already cm^2
            elif unit == "I":
                assert f"Unsupported hail unit {unit}"
            elif unit == "s":
                pass  # already seconds
            else:
                logerr(f"Unknown unit {unit} for {measure}")

        elif measure.endswith("voltage"):
            if unit == "V":
                pass  # already Volt
            else:
                logerr(f"Unknown unit {unit} for {measure}")

        elif measure == "information":
            pass

        else:
            logerr(f"Cannot convert {measure} {value} {unit}")

        return value


class Interface(object):
    MAX_RX_INCOMPLETE_RETRIES = 10
    DEFAULT_TIMEOUT = 2

    def __init__(self):
        pass

    def open(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def write(self, payload: bytes) -> None:
        _ = payload
        raise NotImplementedError

    def readline(self, eol: bytes):
        _ = eol
        raise NotImplementedError

    def flush(self, timeout):
        raise NotImplementedError


class SerialInterface(Interface):
    DEFAULT_PORT = "/dev/ttyUSB0"

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

    def write(self, payload: bytes) -> None:
        logdbg(f"write [{len(payload)}] - {payload}")
        self.serial.write(payload)

    def readline(self, eol: bytes):
        _ = eol
        line = self.serial.readline()
        if line:
            line.replace(b"\x00", b"")  # eliminate any NULL characters
        logdbg(f"readline [{len(line)}] - {line}")
        return line

    def flush(self, timeout):
        pass


class TcpInterface(Interface):
    MAX_CACHED_LINES = 1024

    def __init__(self, host, port, timeout):
        super().__init__()
        self.host = host
        self.port = port
        logdbg(f"Using timeout = {timeout}")
        self.timeout = timeout
        self.socket: socket.socket = None
        self.cached_lines = list()
        self.buffered: bytes = b""

    def open(self):
        self.socket = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP
        )
        self.socket.settimeout(self.timeout)
        loginfo(f"Connecting to {self.host}:{self.port}")
        conn_ex = Exception()
        for _ in range(10):
            try:
                self.socket.connect((self.host, self.port))
                loginfo(f"Connected!")
                break
            except Exception as e:
                logerr(f"Unable to connect to TCP host {self.host}:{self.port}: {e}")
                conn_ex = e
                time.sleep(1)

        else:
            raise ExceptionGroup("Failed to connect", [conn_ex])

        # Preventive flush
        self.flush(1)

    def write(self, payload: bytes) -> None:
        logdbg(f"write [{len(payload)}] - {payload}")
        self.socket.sendall(payload)

    def readline(self, eol: bytes):
        # daft readline implementation - it assumes received packets always
        # contain full lines, delimited by eol.

        if self.cached_lines:
            logdbg(
                f"recv from cached lines - found {len(self.cached_lines)} cached lines"
            )
            retline = self.cached_lines.pop(0)
        else:
            logdbg(f"recv a new line")

            ith_retry = 0
            buf = ""
            for ith_retry in range(Interface.MAX_RX_INCOMPLETE_RETRIES):
                try:
                    buf = self.buffered + self.socket.recv(512)
                except TimeoutError as e:
                    logerr(f"recv timed out: ({e})")
                    return b""

                logdbg(f"readline [{len(buf)}] - {buf}")

                # Check if there is a full line to handle
                if buf.find(eol) >= 0:
                    break

                logwarn(
                    f"Got incomplete line (no EOL) [{len(buf)}] - Buffering and retrying RX [retry {ith_retry}/{Interface.MAX_RX_INCOMPLETE_RETRIES}]"
                )
                self.buffered = buf
            else:
                msg = f"readline - no more partial rx attempts left [{len(buf)}] - [retry {ith_retry}/{Interface.MAX_RX_INCOMPLETE_RETRIES}]"
                logerr(msg)
                raise RuntimeError(msg)

            lines = buf.split(eol)
            assert (
                len(lines) > 0
            ), "We found an EOL before, so there should be at least one line"

            # Whatever is left gets buffered. IF the packet ends with eol this
            # will be an empty string.
            self.buffered = lines.pop(-1)

            if len(lines) == 0:
                logerr("No lines found in received data")
                return b""

            if len(lines) > 1:
                self.cached_lines.extend(lines[1:])

                if len(self.cached_lines) > self.MAX_CACHED_LINES:
                    self.cached_lines = self.cached_lines[-self.MAX_CACHED_LINES :]

            retline = lines[0]

        if not retline:
            logerr(f"Invalid line: {len(retline)} - {retline}")

        return retline

    def close(self):
        self.socket.close()

    def flush(self, timeout):
        r = "1"
        try:
            self.socket.settimeout(timeout)
            while r:
                r = self.socket.recv(512)
                loginfo(f"Flushed {len(r)} bytes")
        except TimeoutError:
            logdbg("Flushing completed")
        finally:
            self.socket.settimeout(self.timeout)


class StationAscii(Station):
    # ASCII over RS232, RS485, and RS422 defaults to 19200, 8, N, 1
    DEFAULT_BAUD = 19200

    def __init__(self, interface, address, use_crc):
        super(StationAscii, self).__init__(interface, address, use_crc)
        self.terminator = "\r\n"


class StationNMEA(Station):
    DEFAULT_BAUD = 4800

    def __init__(self, interface, address, use_crc):
        super().__init__(interface, address, use_crc)
        self.terminator = "\r\n"
        raise NotImplementedError("NMEA support not implemented")


class StationSDI12(Station):
    # SDI12 defaults to 1200, 7, E, 1
    DEFAULT_BAUD = 1200

    def __init__(self, interface, address, use_crc):
        super().__init__(interface, address, use_crc)
        self.terminator = "\r\n"
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

    # Use CRC (1 / 0)
    use_crc = 1

    # timeout for RX
    timeout = 2

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
    MEASURE_2_OBSERVATION = {
        "wind_dir_avg": "windDir",
        "wind_speed_avg": "windSpeed",
        "wind_dir_max": "windGustDir",
        "wind_speed_max": "windGust",
        "temperature": "outTemp",
        "temperature_internal": "extraTemp1",
        "humidity": "outHumidity",
        "pressure": "pressure",
        # Precipitation autosend mode: tipping bucket mode
        # "rain_accumulation": "rain",
        # Precipitation autosend mode: time-interval based
        "rain_accumulation": "totalRain",
        "rain_intensity": "rainRate",
        "rain_duration": "rainDur",
        # NOTE: "rain_intensity_peak" is a mm/h value, while stormRain is
        # group_rain, i.e. it is a cumulative value in mm.
        "hail": "hail",
        "hail_intensity": "hailRate",
        "heating_temperature": "heatingTemp",
        "heating_voltage": "heatingVoltage",
        "supply_voltage": "supplyVoltage",
        "reference_voltage": "referenceVoltage",
    }

    def __init__(self, **stn_dict):
        loginfo("driver version is %s" % DRIVER_VERSION)
        self._model = stn_dict.get("model", "WXT520")
        self._max_tries = int(stn_dict.get("max_tries", 5))
        self._retry_wait = int(stn_dict.get("retry_wait", 10))
        self._poll_interval = int(stn_dict.get("poll_interval", 1))
        self.last_rain_accum: float = 0
        self.last_rain_intensity: float = 0
        self.p_reset_timer: threading.Timer | None = None

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
            port = stn_dict.get("port", SerialInterface.DEFAULT_PORT)
            timeout = stn_dict.get("timeout", Interface.DEFAULT_TIMEOUT)
            interface = SerialInterface(port, baud, timeout)
        elif iface_name == "net":
            host = stn_dict.get("net_host")
            port = stn_dict.get("net_port", 0)
            port = int(port)
            timeout = stn_dict.get("timeout", Interface.DEFAULT_TIMEOUT)
            interface = TcpInterface(host, port, timeout)
        else:
            raise RuntimeError(f"Not a valid interface {iface_name}")

        use_crc = stn_dict.get("use_crc", "")
        use_crc = use_crc in ("1", "true", "True", "yes")

        address = int(stn_dict.get("address", 0))
        self._station = sta_cls(interface, address, use_crc)

        self._station.setup()

    def closePort(self):
        self._station.close()

    @property
    def hardware_name(self):
        return self._model

    def get_loop_packet(self):
        data_msg: str = self._station.get_data_message(self._poll_interval)
        logdbg(f"data message ascii: {data_msg}")
        logdbg(f"data message hex: {hexlify(data_msg.encode(encoding='utf-8'))}")

        if not data_msg:
            raise weewx.WeeWxIOError(f"Got empty data message")

        data_parsed = self._station.parse(data_msg)
        logdbg(f"parsed data message: {pprint.pformat(data_parsed)}")

        if not data_parsed:
            raise weewx.WeeWxIOError(f"No parsed data in data message {data_msg}")

        loop_packet = self.data_to_packet(data_parsed)
        logdbg(f"loop packet: {pprint.pformat(loop_packet)}")

        return loop_packet

    def genLoopPackets(self):
        tries_count = 0

        while True:
            try:
                tries_count += 1
                yield self.get_loop_packet()
                tries_count = 0

            except IOError as e:
                if tries_count >= self._max_tries:
                    raise weewx.WeeWxIOError(
                        f"Gen one loop packet failed after {tries_count} tries: {e}"
                    )

                wait_time_s = tries_count**self._retry_wait
                logerr(
                    f"Failed attempt {tries_count}/{self._max_tries} to read data: {e}. "
                    f"Waiting {wait_time_s}s ..."
                )

                time.sleep(wait_time_s)

    def data_to_packet(self, data: dict) -> dict:
        RAW_PREFIX = "raw_"

        packet = dict()

        assert data, f"No data: {data}"

        for measure in data:
            if measure in self.MEASURE_2_OBSERVATION:
                observation = self.MEASURE_2_OBSERVATION[measure]
                packet[observation] = data[measure]

        # No real observations to report.
        if not packet:
            return packet

        # Also include raw measurements in the loop.
        for measure in data:
            packet[RAW_PREFIX + measure] = data[measure]

        packet["dateTime"] = int(time.time())
        # us = unit system
        packet["usUnits"] = Station.TARGET_UNIT

        ################ Rain
        curr_rain_accum = data.get("rain_accumulation", None)
        curr_rain_intensity = data.get("rain_intensity", None)

        if curr_rain_accum is None:
            logerr("No rain accumulation measure found!")
            return packet
        elif curr_rain_intensity is None:
            logerr("No rain intensity measure found!")
            return packet

        if curr_rain_accum < self.last_rain_accum:
            self.last_rain_accum = 0

        delta_rain = curr_rain_accum - self.last_rain_accum
        self.last_rain_accum = curr_rain_accum
        packet["rain"] = delta_rain

        packet[RAW_PREFIX + "delta_rain"] = delta_rain

        ###### Precipitation ended
        # FIXME: we should lock before calling this!
        def reset_precipitation_callback():
            loginfo("Running precipitation reset timer")
            self._station.precip_counter_reset()
            self._station.precip_intensity_reset()

        if curr_rain_intensity:
            if self.p_reset_timer:
                loginfo("Cancelling precipitation reset timer")
                self.p_reset_timer.cancel()
                self.p_reset_timer = None
        elif self.last_rain_intensity:
            assert not self.p_reset_timer, "Precipitation reset timer found armed!"

            loginfo("Arming precipitation reset timer")
            self.p_reset_timer = threading.Timer(3600, reset_precipitation_callback)
            self.p_reset_timer.start()

        self.last_rain_intensity = curr_rain_intensity

        return packet


# define a main entry point for basic testing of the station without weewx
# engine and service overhead.  invoke this as follows from the weewx root dir:
#
# PYTHONPATH=bin python bin/user/wxt5x0.py

if __name__ == "__main__":
    print("Starting up ...")

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG)
    # set a format which is simpler for console use
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    log.addHandler(console)

    import optparse

    usage = """%prog [options] [--debug] [--help]"""
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("--version", action="store_true", help="display driver version")
    parser.add_option(
        "--debug",
        action="store_true",
        help="display diagnostic information while running",
        default=False,
    )
    parser.add_option("--protocol", help="ascii, nmea, or sdi12", default="ascii")
    parser.add_option(
        "--serial_port",
        help="serial port to which the station is connected",
        default=SerialInterface.DEFAULT_PORT,
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
        "--use-crc",
        action="store_true",
        help="Use CRC on TX - validate CRC on RX",
        default=False,
    )

    parser.add_option(
        "--interface", type=str, help="station interface", default="serial"
    )
    parser.add_option("--net_host", type=str, help="net_host", default="127.0.0.1")
    parser.add_option("--net_port", type=int, help="net port", default=2323)
    parser.add_option(
        "--timeout", type=float, help="RX timeout", default=Interface.DEFAULT_TIMEOUT
    )

    (options, args) = parser.parse_args()

    log.setLevel(logging.DEBUG if options.debug else logging.INFO)
    logdbg("TEST LOG - logdbg")
    loginfo("TEST LOG - loginfo")
    logwarn("TEST LOG - logwarn")
    logerr("TEST LOG - logerr")

    if options.version:
        print("%s driver version %s" % (DRIVER_NAME, DRIVER_VERSION))
        exit(1)

    if options.test_crc:
        print(
            f"string: {options.test_crc} - CRC: '{Station.calc_crc(options.test_crc)}'"
        )
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
        interface = TcpInterface(options.net_host, options.net_port, options.timeout)
    else:
        raise RuntimeError(f"Not a valid interface {options.interface}")

    with sta_cls(interface, options.address, options.use_crc) as s:
        if options.get_wind:
            print("%s" % s.get_wind().strip())
        elif options.get_pth:
            print("%s" % s.get_pth().strip())
        elif options.get_precip:
            print("%s" % s.get_precip().strip())
        elif options.get_supervisor:
            print("%s" % s.get_supervisor().strip())
        elif options.get_composite:
            print("%s" % s.get_composite_data_message().strip())
        else:
            loginfo("Waiting for data ...")
            while True:
                data_msg = s.get_data_message(options.poll_interval)
                logdbg(f"data message ascii: {data_msg}")
                logdbg(
                    f"data message hex: {hexlify(data_msg.encode(encoding='utf-8'))}"
                )
                parsed = Station.parse(data_msg)
                if parsed:
                    loginfo(f"{pprint.pformat(parsed)}")
                else:
                    loginfo(f"[no parsable data]")

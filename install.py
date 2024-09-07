# installer for wxt5x0 driver
# Copyright 2017 Matthew Wall

from weecfg.extension import ExtensionInstaller

def loader():
    return WXT5x0Installer()

class WXT5x0Installer(ExtensionInstaller):
    def __init__(self):
        super(WXT5x0Installer, self).__init__(
            version="2.0",
            name='wxt5x0',
            description='Vaisala WXT520 station driver (prev. Collect data from WXT5x0 hardware)',
            author="Marco Vedovati (prev. Matthew Wall)",
            author_email="mv@sba.lat (prev. mwall@users.sourceforge.net)",
            files=[('bin/user', ['bin/user/wxt5x0.py'])]
        )

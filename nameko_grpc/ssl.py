import ssl


DEFAULT_VERIFY_MODE = "required"
DEFAULT_CHECK_HOSTNAME = True
DEFAULT_SSL_CONFIG = {
    "verify_mode": DEFAULT_VERIFY_MODE,
    "check_hostname": DEFAULT_CHECK_HOSTNAME,
}


class SslConfig:
    def __init__(self, config):
        if config is True:
            config = DEFAULT_SSL_CONFIG
        self.config = config

    def __bool__(self):
        return bool(self.config)

    @property
    def verify_mode(self):
        if not self.config:
            return None

        value = self.config.get("verify_mode", DEFAULT_VERIFY_MODE)
        verify_modes = {
            "none": ssl.CERT_NONE,
            "optional": ssl.CERT_OPTIONAL,
            "required": ssl.CERT_REQUIRED,
        }
        return verify_modes[value]

    @property
    def check_hostname(self):
        if not self.config:
            return None

        return self.config.get("check_hostname", DEFAULT_CHECK_HOSTNAME)

    @property
    def verify_locations(self):
        return self.config.get("verify_locations", None)

    @property
    def cert_chain(self):
        return self.config.get("cert_chain", None)

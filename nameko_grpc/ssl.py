# -*- coding: utf-8 -*-
import ssl


DEFAULT_VERIFY_MODE = "required"
DEFAULT_CHECK_HOSTNAME = True
DEFAULT_SSL_CONFIG = {
    "verify_mode": DEFAULT_VERIFY_MODE,
    "check_hostname": DEFAULT_CHECK_HOSTNAME,
}


class SslConfig:
    def __init__(self, config):
        """Valid values for `config` are:

        - True
        - False
        - A dict with the following format, all keys optional:

            {
                "verify_mode": <"none"|"optional"|"required">,
                "check_hostname": <True|False>,
                "verify_locations": {
                    <args for SslContext.load_verify_locations>
                },
                "cert_chain": {
                    <args for SslContext.load_cert_chain>
                }
            }
        """
        # TODO add schema for this config dict
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

    def server_context(self):
        """Returns a configured context for use in a server."""
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        if self.cert_chain:
            context.load_cert_chain(**self.cert_chain)
        context.set_alpn_protocols(["h2"])
        return context

    def client_context(self):
        """Returns a configured context for use in a client."""
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        context.check_hostname = self.check_hostname
        context.verify_mode = self.verify_mode
        context.set_alpn_protocols(["h2"])
        return context

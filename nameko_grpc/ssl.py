import ssl


class SslConfig:
    def __init__(
        self,
        verify_mode=ssl.CERT_REQUIRED,
        check_hostname=True,
        verify_locations=None,
        cert_chain=None,
    ):
        self.verify_mode = verify_mode
        self.check_hostname = check_hostname
        self.verify_locations = verify_locations
        self.cert_chain = cert_chain

    @staticmethod
    def from_dict(dict_config):
        if dict_config is False:
            return False
        if dict_config is True:
            dict_config = {}  # use defaults
        verify_modes = {
            "none": ssl.CERT_NONE,
            "optional": ssl.CERT_OPTIONAL,
            "required": ssl.CERT_REQUIRED,
        }
        verify_mode = verify_modes[dict_config.get("verify_mode", "required")]
        check_hostname = dict_config.get("check_hostname", True)
        verify_locations = dict_config.get("verify_locations", None)
        cert_chain = dict_config.get("cert_chain", None)

        return SslConfig(verify_mode, check_hostname, verify_locations, cert_chain)

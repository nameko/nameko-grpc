# -*- coding: utf-8 -*-
def pytest_addoption(parser):

    parser.addoption(
        "--client",
        action="store",
        type="choice",
        choices=["nameko", "grpc", "all"],
        dest="client",
        default="all",
        help="Use this client type",
    )

    parser.addoption(
        "--server",
        action="store",
        type="choice",
        choices=["nameko", "grpc", "all"],
        dest="server",
        default="all",
        help="Use this server type",
    )

package(
    default_visibility = ["PUBLIC"],
    python = {
        "WHEEL_REPO": "https://get.please.build/third_party/python/py3",
        "WHEEL_NAME_SCHEME": [
            "{url_base}/{package_name}-{version}-${{OS}}_${{ARCH}}.whl",
            "{url_base}/{package_name}-{version}.whl",
            "https://files.pythonhosted.org/packages/py3/{initial}/{package_name}/{package_name}-{version}-py3-none-any.whl",
            "https://files.pythonhosted.org/packages/py2.py3/{initial}/{package_name}/{package_name}-{version}-py2.py3-none-any.whl",
        ],
    },
)

python_wheel(
    name = "protobuf",
    outs = ["google"],
    version = "7.34.1",
)

pip_library(
    name = "grpc",
    package_name = "grpcio",
    licences = ["Apache-2.0"],
    version = "1.80.0",
    zip_safe = False,  # Contains Cython code in a .so
    deps = [
        ":six",
    ],
)

pip_library(
    name = "grpclib",
    licences = ["BSD-3-Clause"],
    version = "0.4.9",
    deps = [
        ":h2",
        ":multidict",
        ":protobuf",
    ],
)

python_wheel(
    name = "six",
    outs = ["six.py"],
    version = "1.17.0",
)

pip_library(
    name = "h2",
    licences = ["MIT"],
    version = "4.3.0",
    deps = [":hpack"],
)

pip_library(
    name = "hpack",
    licences = ["MIT"],
    version = "4.1.0",
)

pip_library(
    name = "multidict",
    licences = ["Apache-2.0"],
    version = "6.7.1",
    deps = [":hyperframe"],
)

pip_library(
    name = "hyperframe",
    licences = ["MIT"],
    version = "6.1.0",
)

pip_library(
    name = "typing_extensions",
    licences = ["PSF-2.0"],
    version = "4.15.0",
)

pip_library(
    name = "protovalidate",
    package_name = "protovalidate",
    licences = ["Apache-2.0"],
    version = "1.2.0",
    deps = [":protobuf"],
)

package(default_visibility = ["PUBLIC"])

pip_library(
    name = "pkg_resources",
    package_name = "setuptools",
    version = "40.4.3",
)

pip_library(
    name = "enum",
    package_name = "enum34",
    version = "1.1.6",
)

pip_library(
    name = "grafanalib",
    version = "0.6.1",
    deps = [":attrs"],
)

pip_library(
    name = "attrs",
    package_name = "attrs",
    version = "21.4.0",
)

pip_library(
    name = "six",
    version = "1.10.0",
)

pip_library(
    name = "protobuf",
    version = "3.19.4",
    deps = [":six"],
)

pip_library(
    name = "grpc",
    package_name = "grpcio",
    version = "1.43.0",
    zip_safe = False,  # Contains Cython code in a .so
    deps = [
        ":enum",
        ":futures",
        ":six",
    ],
)

pip_library(
    name = "grpc-tools",
    package_name = "grpcio-tools",
    version = "1.8.6",
    zip_safe = False,  # Contains Cython code in a .so
)

pip_library(
    name = "futures",
    version = "3.1.1",
)

pip_library(
    name = "plotly",
    version = "5.6.0",
    deps = [":tenacity"],
)

pip_library(
    name = "tenacity",
    version = "8.0.1",
)

pip_library(
    name = "pandas",
    version = "1.4.1",
    deps = [
        ":numpy",
        ":python-dateutil",
        ":pytz",
    ],
)

pip_library(
    name = "numpy",
    version = "1.22.3",
)

pip_library(
    name = "pytz",
    version = "2021.3",
)

pip_library(
    name = "python-dateutil",
    version = "2.8.2",
)

pip_library(
    name = "dash",
    version = "2.1.0",
    deps = [
        ":flask",
        ":setuptools",
    ],
)

pip_library(
    name = "flask",
    version = "2.0.3",
    deps = [
        ":Flask-Compress",
        ":click",
        ":itsdangerous",
        ":jinja2",
        ":markupsafe",
        ":werkzeug",
    ],
)

pip_library(
    name = "markupsafe",
    version = "2.0.1",
)

pip_library(
    name = "werkzeug",
    version = "2.0.3",
)

pip_library(
    name = "jinja2",
    version = "3.0.3",
)

pip_library(
    name = "click",
    version = "8.0.1",
)

pip_library(
    name = "itsdangerous",
    version = "2.0.1",
)

pip_library(
    name = "Flask-Compress",
    version = "1.10.1",
    deps = [":Brotli"],
)

pip_library(
    name = "Brotli",
    version = "1.0.9",
)

pip_library(
    name = "setuptools",
    version = "60.9.1",
)

pip_library(
    name = "waitress",
    version = "2.0.0",
)

import pkg_resources

common_settings = {'dict_parameter_format': 'json'}


def version():
    return pkg_resources.get_distribution('clickhouse-connect').version

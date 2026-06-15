import configparser

def get_config():
    config = configparser.ConfigParser()
    config.read("conf/proj.conf")
    conf = {}
    for (key, val) in config.items("KAFKA"):
        conf[key] = val
    return conf
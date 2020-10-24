import logging
import os
import shutil
import sys
import zipfile
from logging.config import fileConfig

import jinja2
import pytest
import yaml
from paver.easy import *
from pylint import lint

sys.path.append(os.path.join(os.path.dirname(__file__), ""))

import scb.recommender.als as als
import scb.etl.pipeline as etl_pipeline

DEFAULT_LINT_OPTIONS = ["--rcfile", "pylint.cfg", "-r", "yes", "--suggestion-mode", "y"]
fileConfig('logger_conf.ini')
LOGGER = logging.getLogger()


class Config:
    def __init__(self, source_file: str, template_vars: dict):
        with open(source_file, 'r') as fh:
            template_conf = jinja2.Template(fh.read())

        conf_dict = yaml.load(template_conf.render(**template_vars), yaml.FullLoader)
        for k, v in conf_dict.items():
            self.__setattr__(k, v)


def load_config(deploy_env):
    return Config("deployment.yaml", {'deploy_env': deploy_env})


def zip_dir(mod_path, zip_fh):
    for root, dirs, files in os.walk(mod_path):
        if "__pycache__" in root:
            LOGGER.info("Skipping dir {} with files: {}".format(root, ",".join(files)))
            continue
        for file in files:
            LOGGER.info(f"Adding {file}")
            zip_fh.write(os.path.join(root, file))


@task
@needs("py_lint")
def package():
    config = load_config("dev")
    if os.path.isdir(config.target_dir):
        shutil.rmtree(config.target_dir)

    os.mkdir(config.target_dir)

    lib_zip = os.path.join(config.target_dir, "lib.zip")
    with zipfile.ZipFile(lib_zip, 'w', zipfile.ZIP_DEFLATED) as zip_fh:
        for mod in config.modules:
            mod_dir = os.path.join(".", mod)
            zip_dir(mod_dir, zip_fh)

    for f in config.entry_points:
        shutil.copy(f, os.path.join(config.target_dir, os.path.basename(f)))


@task
@needs("package")
@cmdopts([('environment=', 'e', 'Environment where to deploy stg prd'),
          ('version=', 'v', 'Version name to deploy')
          ]
         )
def deploy():
    if hasattr(options, 'environment'):
        deploy_env = options.environment
    else:
        deploy_env = "stg"
    if hasattr(options, 'version'):
        version = options.version
    else:
        version = "latest"

    config = load_config(deploy_env)


@task
def test():
    exit_code = pytest.main(["-x", "--cov-report", "term-missing", "--cov=scb", "tests"])
    if exit_code != pytest.ExitCode.OK:
        raise paver.tasks.BuildFailure('pytest finished with a non-zero exit code')


@task
def py_lint():
    """Check the source code using PyLint."""
    config = load_config("dev")
    for mod in config.modules:
        arguments = []
        arguments.extend(DEFAULT_LINT_OPTIONS)
        arguments.append(mod)

        try:
            lint.Run(arguments)
        # PyLint will `sys.exit()` when it has finished, so we need to catch
        # the exception and process it accordingly.
        except SystemExit as exc:
            return_code = exc.args[0]


@task
def model_training():
    als.Pipeline().run()


@task
def etl():
    etl_pipeline.Pipeline.run()

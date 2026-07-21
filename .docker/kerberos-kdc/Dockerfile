FROM rockylinux:9

RUN yum install -y ca-certificates krb5-server krb5-libs krb5-workstation

EXPOSE 88 749

RUN touch /config.sh
# Overwritten via the docker-compose volume mount -- see tests/integration_tests/kerberos_conf/kerberos_image_config.sh

ENTRYPOINT ["/bin/bash", "/config.sh"]

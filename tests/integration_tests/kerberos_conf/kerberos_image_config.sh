#!/bin/bash


set -x # trace

: "${REALM:=TEST.CLICKHOUSE.TECH}"
: "${DOMAIN_REALM:=test.clickhouse.com}"
: "${KERB_MASTER_KEY:=masterkey}"
: "${KERB_ADMIN_USER:=admin}"
: "${KERB_ADMIN_PASS:=admin}"

create_config() {
  : "${KDC_ADDRESS:=$(hostname -f)}"

  cat>/etc/krb5.conf<<EOF
[logging]
 default = FILE:/var/log/kerberos/krb5libs.log
 kdc = FILE:/var/log/kerberos/krb5kdc.log
 admin_server = FILE:/var/log/kerberos/kadmind.log

[libdefaults]
 default_realm = $REALM
 dns_lookup_realm = false
 dns_lookup_kdc = false
 ticket_lifetime = 15s
 renew_lifetime = 15s
 forwardable = true
 default_tkt_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96
 default_tgs_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96
 permitted_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96

[realms]
 $REALM = {
  kdc = $KDC_ADDRESS
  admin_server = $KDC_ADDRESS
 }

[domain_realm]
 .$DOMAIN_REALM = $REALM
 $DOMAIN_REALM = $REALM
EOF

cat>/var/kerberos/krb5kdc/kdc.conf<<EOF
[kdcdefaults]
 kdc_ports = 88
 kdc_tcp_ports = 88

[realms]
 $REALM = {
  acl_file = /var/kerberos/krb5kdc/kadm5.acl
  dict_file = /usr/share/dict/words
  admin_keytab = /var/kerberos/krb5kdc/kadm5.keytab
  master_key_type = aes256-cts-hmac-sha1-96
  supported_enctypes = aes256-cts-hmac-sha1-96:normal aes128-cts-hmac-sha1-96:normal
  default_principal_flags = +preauth
 }
EOF
}

create_db() {
  /usr/sbin/kdb5_util -P $KERB_MASTER_KEY -r $REALM create -s
}

start_kdc() {
  mkdir -p /var/log/kerberos

  /usr/sbin/krb5kdc
  /usr/sbin/kadmind
}

restart_kdc() {
  pkill krb5kdc
  pkill kadmind
  start_kdc
}

create_admin_user() {
  kadmin.local -q "addprinc -pw $KERB_ADMIN_PASS $KERB_ADMIN_USER/admin"
  echo "*/admin@$REALM *" > /var/kerberos/krb5kdc/kadm5.acl
}

create_keytabs() {
  rm /tmp/keytab/*.keytab

  kadmin.local -q "addprinc -randkey kuser@${REALM}"
  kadmin.local -q "ktadd -norandkey -k /tmp/keytab/kuser.keytab kuser@${REALM}"

  kadmin.local -q "addprinc -randkey HTTP/server1.clickhouse.test@${REALM}"
  kadmin.local -q "ktadd -norandkey -k /tmp/keytab/server1.clickhouse.test.keytab HTTP/server1.clickhouse.test@${REALM}"

  chmod g+r /tmp/keytab/kuser.keytab
  chmod g+r /tmp/keytab/server1.clickhouse.test.keytab
}

main() {

  if [ ! -f /kerberos_initialized ]; then
    create_config
    create_db
    create_admin_user
    start_kdc

    touch /kerberos_initialized
  fi

  if [ ! -f /var/kerberos/krb5kdc/principal ]; then
    while true; do sleep 1000; done
  else
    start_kdc
		create_keytabs
    tail -F /var/log/kerberos/krb5kdc.log
  fi

}

[[ "$0" == "${BASH_SOURCE[0]}" ]] && main "$@"

FROM rockylinux:9

# This is to solve permission issue, read https://denibertovic.com/posts/handling-permissions-with-docker-volumes/
RUN curl -o /usr/local/bin/gosu -SL "https://github.com/tianon/gosu/releases/download/1.14/gosu-amd64"
RUN chmod +x /usr/local/bin/gosu

COPY scripts/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

WORKDIR /home/user
RUN chmod 777 /home/user

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

RUN dnf clean all && \
    dnf groupinstall -y "Development Tools" && \
    dnf install -y redhat-rpm-config gcc libffi-devel wget && \
    dnf install -y perl perl-IPC-Cmd perl-Digest-SHA perl-Test-Simple perl-Pod-Html python3.11 python3.11-devel python3.11-pip && \
    dnf clean all && \
    rm -rf /var/cache/dnf

# build, install, and enable openssl 3.0.0 FIPS
RUN wget https://www.openssl.org/source/openssl-3.0.0.tar.gz && \
    tar -zxf openssl-3.0.0.tar.gz && \
    cd openssl-3.0.0 && \
    ./Configure enable-fips && \
    make > /dev/null && \
    make install > /dev/null && \
    make install_fips > /dev/null && \
    FIPS_MODULE="$(find /usr/local -path '*/ossl-modules/fips.so' | head -1)" && \
    LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib64:/usr/local/lib /usr/local/bin/openssl fipsinstall \
        -out /usr/local/ssl/fipsmodule.cnf -module "$FIPS_MODULE"
RUN cat <<EOF >> /usr/local/ssl/openssl.cnf
openssl_conf = openssl_init

.include /usr/local/ssl/fipsmodule.cnf

[openssl_init]
providers = provider_sect

[provider_sect]
fips = fips_sect
base = base_sect

[base_sect]
activate = 1
EOF

RUN python3.11 -m pip install --upgrade pip setuptools wheel

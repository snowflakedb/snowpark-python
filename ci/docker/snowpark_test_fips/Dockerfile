FROM centos:8

# This is to solve permission issue, read https://denibertovic.com/posts/handling-permissions-with-docker-volumes/
RUN curl -o /usr/local/bin/gosu -SL "https://github.com/tianon/gosu/releases/download/1.14/gosu-amd64"
RUN chmod +x /usr/local/bin/gosu

COPY scripts/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

WORKDIR /home/user
RUN chmod 777 /home/user

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

RUN sed -i s/mirror.centos.org/vault.centos.org/g /etc/yum.repos.d/*.repo && \
    sed -i s/^#.*baseurl=http/baseurl=http/g /etc/yum.repos.d/*.repo && \
    sed -i s/^mirrorlist=http/#mirrorlist=http/g /etc/yum.repos.d/*.repo

RUN yum clean all && \
    yum groupinstall -y "Development Tools" && \
    yum install -y redhat-rpm-config gcc libffi-devel wget && \
    yum install -y perl-IPC-Cmd perl-Digest-SHA perl-Test-Simple perl-Pod-Html python39 python39-devel && \
    yum clean all && \
    rm -rf /var/cache/yum

# build openssl 3.0.0
RUN wget https://www.openssl.org/source/openssl-3.0.0.tar.gz && \
    tar -zxf openssl-3.0.0.tar.gz && \
    cd openssl-3.0.0 && \
    ./Configure enable-fips && \
    make > /dev/null

# install openssl 3.0.0
RUN cd openssl-3.0.0 && \
    make install > /dev/null && \
    make install_fips > /dev/null

# enable openssl fips mode
RUN LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib64 openssl fipsinstall -out /usr/local/ssl/fipsmodule.cnf -module /usr/local/lib64/ossl-modules/fips.so
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

RUN python3 -m pip install --user --upgrade pip setuptools wheel

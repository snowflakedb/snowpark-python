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
    yum install -y redhat-rpm-config gcc sudo libffi-devel wget && \
    yum install -y perl-IPC-Cmd perl-Digest-SHA perl-Test-Simple perl-Pod-Html python38 python38-devel && \
    yum clean all && \
    rm -rf /var/cache/yum

RUN wget https://www.openssl.org/source/openssl-3.0.0.tar.gz && \
    tar -zxf openssl-3.0.0.tar.gz && \
    cd openssl-3.0.0 && \
    ./Configure enable-fips && \
    make

RUN cd openssl-3.0.0 && mkdir -p /usr/local/ssl/ && sudo make install && sudo make install_fips && \
    ln -s /usr/local/lib/libssl.so.3 /usr/lib/libssl.so.3 && \
    ln -s /usr/local/lib/libcrypto.so.3 /usr/lib/libcrypto.so.3


RUN echo $PATH && ls /usr/local/bin && LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib openssl fipsinstall -out /usr/local/ssl/fipsmodule.cnf -module /usr/local/lib/ossl-modules/fips.so

RUN python3 -m pip install --user --upgrade pip setuptools wheel

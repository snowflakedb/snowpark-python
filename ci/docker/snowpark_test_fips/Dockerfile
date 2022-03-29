FROM centos:7

# This is to solve permission issue, read https://denibertovic.com/posts/handling-permissions-with-docker-volumes/
RUN curl -o /usr/local/bin/gosu -SL "https://github.com/tianon/gosu/releases/download/1.11/gosu-amd64"
RUN chmod +x /usr/local/bin/gosu

COPY scripts/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

WORKDIR /home/user
RUN chmod 777 /home/user

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

RUN yum install -y python3 redhat-rpm-config gcc libffi-devel python3-devel openssl openssl-devel
RUN cd /opt && wget --no-check-certificate https://www.python.org/ftp/python/3.8.12/Python-3.8.12.tgz && tar xzf Python-3.8.12.tgz && cd Python-3.8*/ && ./configure --enable-optimizations && make altinstall && rm -rf /opt/Python*
RUN python3.8 -m pip install --upgrade pip setuptools wheel
RUN python3.8 -m pip install virtualenv

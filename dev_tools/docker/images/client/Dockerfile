#----------------------------------------------------
FROM crs4_pydoop/base:latest

#----------------------------------
# Install useful stuff
# NO update. We should be in line with base
RUN apt-get install -y git build-essential python-dev

#----------------------------------
# Enable sshd
RUN mkdir /var/run/sshd
RUN echo 'root:hadoop' | chpasswd
RUN sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# SSH login fix. Otherwise user is kicked off after login
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

EXPOSE 22

#-----------------------------------
CMD ["/usr/sbin/sshd", "-D"]
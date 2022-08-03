# ================================ VIDI Base ================================
FROM splunk/splunk:8.1.0.1 as vidi_base

# Specify the openjdk version to be used and pre-install it to speedup docker runs
ENV JAVA_VERSION=openjdk:8
USER root
# Reference: https://github.com/splunk/splunk-ansible/blob/develop/roles/splunk_common/tasks/java_tasks/install_openjdk8_jdk.yml
# Install gettext for envsubst command in our entrypoint
# RUN microdnf install java-1.8.0-openjdk gettext
USER ansible


# ================================ VIDI Standalone HTTP ================================
FROM vidi_base as vidi_standalone_http

# Copy search metadata for comments in searches
COPY --chown=splunk:splunk ./vidi_standalone/apps/ /opt/splunk/etc/apps/


# ================================ VIDI Standalone HTTPS ================================
FROM vidi_standalone_http as vidi_standalone_https

# Configurations used by Ansible in the container
COPY --chown=ansible:ansible ./vidi_standalone/default.yml /tmp/defaults/default.yml


# ================================ VIDI Heavy Forwarder ================================
FROM vidi_base as vidi_heavy_forwarder

# Copy Heavy Forwarder configuration for Deployment Server
COPY --chown=splunk:splunk ./vidi_heavy_forwarder/. /tmp/vidi_heavy_forwarder/.

ENTRYPOINT [ "/tmp/vidi_heavy_forwarder/entrypoint.sh" ]
CMD [ "start-service" ]

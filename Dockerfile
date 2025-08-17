FROM mcr.microsoft.com/devcontainers/rust:1-1-bookworm

# Copy sqlc bin
COPY --from=sqlc/sqlc:1.29.0 /workspace/sqlc /usr/bin/sqlc

ARG USERNAME=vscode
USER ${USERNAME}

# Add completions
RUN echo "source /usr/share/bash-completion/completions/git" >> /home/${USERNAME}/.bashrc
RUN echo "source <( rustup completions bash )" >> /home/${USERNAME}/.bashrc
RUN echo "source <( rustup completions bash cargo )" >> /home/${USERNAME}/.bashrc

RUN cargo install just sqlx-cli

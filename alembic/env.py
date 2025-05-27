from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context

from app.config import settings
from app.database import Base
from app.models import datapoints  # Ensure all models are imported and registered

import re

# Alembic Config object
config = context.config

# Inject DB URL from settings
config.set_main_option("sqlalchemy.url", settings.sqlalchemy_database_uri.replace('%', '%%'))

# Logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Metadata of models to include in migrations
target_metadata = Base.metadata

def should_include_object(object, name, type_, reflected, compare_to):
    """
    Custom rule: exclude tables that start with 'table_' (dynamic batch uploads).
    """
    if type_ == "table" and name.startswith("table_"):
        return False
    return True

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=should_include_object
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=should_include_object
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

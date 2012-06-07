mysql_sniper
============

A set of python scripts to kill long running transactions.

Uses yaml for configurations. Configuration file obviously not committed publicly. A YAML config might look like this:

production:
        user: with_process_privileges
        password: an_awesome_password
        TTL: 60


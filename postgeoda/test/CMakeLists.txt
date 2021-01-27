set(PRIMARY_TEST_DIR ${CMAKE_CURRENT_LIST_DIR})
set(PRIMARY_TEST_DIR ${CMAKE_CURRENT_LIST_DIR} PARENT_SCOPE)
set(_local_install_checks)
set(_install_checks)

# Testing support
find_program(PG_REGRESS pg_regress
        HINTS
        "${PG_PKGLIBDIR}/pgxs/src/test/regress/")

find_program(PG_ISOLATION_REGRESS
        NAMES pg_isolation_regress
        HINTS
        ${BINDIR}
        ${PG_PKGLIBDIR}/pgxs/src/test/isolation
        ${PG_SOURCE_DIR}/src/test/isolation)


set(TEST_ROLE_SUPERUSER super_user)
set(TEST_ROLE_DEFAULT_PERM_USER default_perm_user)
set(TEST_ROLE_DEFAULT_PERM_USER_2 default_perm_user_2)

set(TEST_INPUT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(TEST_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR})
set(TEST_CLUSTER ${TEST_OUTPUT_DIR}/testcluster)

# Basic connection info for test instance
set(TEST_PGPORT_LOCAL 5432 CACHE STRING "The port of a running PostgreSQL instance")
set(TEST_PGHOST localhost CACHE STRING "The hostname of a running PostgreSQL instance")
set(TEST_PGUSER ${TEST_ROLE_DEFAULT_PERM_USER} CACHE STRING "The PostgreSQL test user")
set(TEST_DBNAME single CACHE STRING "The database name to use for tests")
set(TEST_PGPORT_TEMP_INSTANCE 15432 CACHE STRING "The port to run a temporary test PostgreSQL instance on")
set(TEST_SCHEDULE ${CMAKE_CURRENT_BINARY_DIR}/test_schedule)
set(TEST_SCHEDULE_SHARED ${CMAKE_CURRENT_BINARY_DIR}/shared/test_schedule_shared)
set(ISOLATION_TEST_SCHEDULE ${CMAKE_CURRENT_BINARY_DIR}/isolation_test_schedule)

set(PG_REGRESS_OPTS_BASE
        --host=${TEST_PGHOST}
        --load-language=plpgsql
        --dlpath=${PROJECT_BINARY_DIR}/src)

set(PG_REGRESS_OPTS_EXTRA
        --create-role=${TEST_ROLE_SUPERUSER},${TEST_ROLE_DEFAULT_PERM_USER},${TEST_ROLE_DEFAULT_PERM_USER_2}
        --dbname=${TEST_DBNAME}
        --launcher=${PRIMARY_TEST_DIR}/runner.sh)

if(PG_REGRESS)
    set(PG_REGRESS_ENV
            TEST_PGUSER=${TEST_PGUSER}
            TEST_PGHOST=${TEST_PGHOST}
            TEST_ROLE_SUPERUSER=${TEST_ROLE_SUPERUSER}
            TEST_ROLE_DEFAULT_PERM_USER=${TEST_ROLE_DEFAULT_PERM_USER}
            TEST_ROLE_DEFAULT_PERM_USER_2=${TEST_ROLE_DEFAULT_PERM_USER_2}
            TEST_DBNAME=${TEST_DBNAME}
            TEST_INPUT_DIR=${TEST_INPUT_DIR}
            TEST_OUTPUT_DIR=${TEST_OUTPUT_DIR}
            TEST_SCHEDULE=${TEST_SCHEDULE}
            PG_BINDIR=${PG_BINDIR}
            PG_REGRESS=${PG_REGRESS})
endif()

if (${PG_VERSION_MAJOR} GREATER "9")
    set(TEST_VERSION_SUFFIX ${PG_VERSION_MAJOR})
else ()
    set(TEST_VERSION_SUFFIX ${PG_VERSION_MAJOR}.${PG_VERSION_MINOR})
endif ()

if(PG_REGRESS)
    message(STATUS "Using pg_regress ${PG_REGRESS}")

    add_custom_target(regresscheck
            COMMAND ${CMAKE_COMMAND} -E env
            ${PG_REGRESS_ENV}
            ${CMAKE_CURRENT_SOURCE_DIR}/pg_regress.sh
            ${PG_REGRESS_OPTS_BASE}
            ${PG_REGRESS_OPTS_EXTRA}
            ${PG_REGRESS_OPTS_INOUT}
            ${PG_REGRESS_OPTS_TEMP_INSTANCE}
            USES_TERMINAL)
    list(APPEND _install_checks regresscheck)
endif()
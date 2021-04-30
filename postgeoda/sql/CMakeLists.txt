set(INSTALL_FILE ${PROJECT_NAME}--${PROJECT_VERSION_MOD}.sql)


# The rest of the source files defining mostly functions
set(SOURCE_FILES
        weights.sql
        weights_knn.sql
        weights_dist.sql
        moran.sql
        g.sql
        joincount.sql
        quantilelisa.sql
        breaks.sql
        )

set(MODULE_PATHNAME "$libdir/postgeoda-${PROJECT_VERSION_MOD}")
set(LOADER_PATHNAME "$libdir/postgeoda")

# Function to replace @MODULE_PATHNAME@ in source files, producing an
# output file in the build dir
function(version_files SRC_FILE_LIST OUTPUT_FILE_LIST)
    set(result "")
    foreach(unversioned_file ${SRC_FILE_LIST})
        set(versioned_file ${unversioned_file})
        list(APPEND result ${CMAKE_CURRENT_BINARY_DIR}/${versioned_file})
        if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${unversioned_file})
            configure_file(${unversioned_file} ${versioned_file} @ONLY)
        endif ()
    endforeach(unversioned_file)
    set(${OUTPUT_FILE_LIST} "${result}" PARENT_SCOPE)
endfunction()

version_files("${SOURCE_FILES}" SOURCE_FILES_VERSIONED)

# Function to concatenate all files in SRC_FILE_LIST into file OUTPUT_FILE
function(cat_files SRC_FILE_LIST OUTPUT_FILE)
    set(CAT_CMD cat ${SRC_FILE_LIST} > ${OUTPUT_FILE})
    add_custom_command(
            OUTPUT ${OUTPUT_FILE}
            DEPENDS ${SRC_FILE_LIST}
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            COMMAND ${CAT_CMD}
            COMMENT "Generating ${OUTPUT_FILE}"
    )
endfunction()

# Generate the extension file used with CREATE EXTENSION
cat_files(
        "${SOURCE_FILES_VERSIONED}"
        ${CMAKE_CURRENT_BINARY_DIR}/${INSTALL_FILE}
)
add_custom_target(sqlfile ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/${INSTALL_FILE})

# Install target for the extension file and update scripts
install(
        FILES ${CMAKE_CURRENT_BINARY_DIR}/${INSTALL_FILE}
        ${UPDATE_SCRIPTS}
        DESTINATION "${PG_SHAREDIR}/extension")
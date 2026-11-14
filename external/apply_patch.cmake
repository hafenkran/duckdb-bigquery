cmake_minimum_required(VERSION 3.20)

if(NOT IS_DIRECTORY "${SOURCE_DIR}" OR NOT EXISTS "${PATCH_FILE}")
    message(
        FATAL_ERROR "SOURCE_DIR and PATCH_FILE must identify existing paths")
endif()

find_program(PATCH_EXECUTABLE patch REQUIRED)

execute_process(
    COMMAND "${PATCH_EXECUTABLE}" --help
    OUTPUT_VARIABLE PATCH_HELP
    ERROR_VARIABLE PATCH_HELP_ERROR)
if("${PATCH_HELP}${PATCH_HELP_ERROR}" MATCHES "--dry-run")
    set(PATCH_DRY_RUN --dry-run)
else()
    set(PATCH_DRY_RUN -C)
endif()

execute_process(
    COMMAND "${PATCH_EXECUTABLE}" -p1 -t -N -F 0 ${PATCH_DRY_RUN} -i
            "${PATCH_FILE}"
    WORKING_DIRECTORY "${SOURCE_DIR}"
    RESULT_VARIABLE PATCH_CHECK_RESULT
    OUTPUT_VARIABLE PATCH_CHECK_OUTPUT
    ERROR_VARIABLE PATCH_CHECK_ERROR)

if(PATCH_CHECK_RESULT EQUAL 0)
    execute_process(
        COMMAND "${PATCH_EXECUTABLE}" -p1 -t -N -F 0 -i "${PATCH_FILE}"
        WORKING_DIRECTORY "${SOURCE_DIR}"
        RESULT_VARIABLE PATCH_RESULT
        OUTPUT_VARIABLE PATCH_OUTPUT
        ERROR_VARIABLE PATCH_ERROR)
    if(NOT
       PATCH_RESULT
       EQUAL
       0)
        message(
            FATAL_ERROR
                "Could not apply ${PATCH_FILE}:\n${PATCH_OUTPUT}${PATCH_ERROR}")
    endif()
else()
    # Accept an already-applied patch only if every hunk can be reversed.
    execute_process(
        COMMAND "${PATCH_EXECUTABLE}" -p1 -t -N -R -F 0 ${PATCH_DRY_RUN} -i
                "${PATCH_FILE}"
        WORKING_DIRECTORY "${SOURCE_DIR}"
        RESULT_VARIABLE PATCH_REVERSE_RESULT
        OUTPUT_VARIABLE PATCH_REVERSE_OUTPUT
        ERROR_VARIABLE PATCH_REVERSE_ERROR)
    if(NOT
       PATCH_REVERSE_RESULT
       EQUAL
       0)
        message(
            FATAL_ERROR
                "Patch neither applies nor is already applied: ${PATCH_FILE}\n"
                "${PATCH_CHECK_OUTPUT}${PATCH_CHECK_ERROR}"
                "${PATCH_REVERSE_OUTPUT}${PATCH_REVERSE_ERROR}")
    endif()
    message(STATUS "Patch already applied: ${PATCH_FILE}")
endif()

#!/usr/bin/env bash

set +e
declare builtin_sbt_version="1.5.1"
declare -a residual_args
declare -a java_args
declare -a scalac_args
declare -a sbt_commands
declare -a sbt_options
declare -a print_version
declare -a print_sbt_version
declare -a print_sbt_script_version
declare -a original_args
declare java_cmd=java
declare java_version
declare init_sbt_version=1.5.1
declare sbt_default_mem=1024
declare -r default_sbt_opts=""
declare -r default_java_opts="-Dfile.encoding=UTF-8"
declare sbt_verbose=
declare sbt_debug=
declare build_props_sbt_version=
declare use_sbtn=
declare sbtn_command="$SBTN_CMD"

###  ------------------------------- ###
###  Helper methods for BASH scripts ###
###  ------------------------------- ###

# Bash reimplementation of realpath to return the absolute path
realpathish () {
(
  TARGET_FILE="$1"
  FIX_CYGPATH="$2"

  cd "$(dirname "$TARGET_FILE")"
  TARGET_FILE=$(basename "$TARGET_FILE")

  COUNT=0
  while [ -L "$TARGET_FILE" -a $COUNT -lt 100 ]
  do
    TARGET_FILE=$(readlink "$TARGET_FILE")
    cd "$(dirname "$TARGET_FILE")"
    TARGET_FILE=$(basename "$TARGET_FILE")
    COUNT=$(($COUNT + 1))
  done

  # make sure we grab the actual windows path, instead of cygwin's path.
  if [[ "x$FIX_CYGPATH" != "x" ]]; then
    echo "$(cygwinpath "$(pwd -P)/$TARGET_FILE")"
  else
    echo "$(pwd -P)/$TARGET_FILE"
  fi
)
}

# Uses uname to detect if we're in the odd cygwin environment.
is_cygwin() {
  local os=$(uname -s)
  case "$os" in
    CYGWIN*) return 0 ;;
    MINGW*) return 0 ;;
    MSYS*) return 0 ;;
    *)  return 1 ;;
  esac
}

# TODO - Use nicer bash-isms here.
CYGWIN_FLAG=$(if is_cygwin; then echo true; else echo false; fi)

# This can fix cygwin style /cygdrive paths so we get the
# windows style paths.
cygwinpath() {
  local file="$1"
  if [[ "$CYGWIN_FLAG" == "true" ]]; then #"
    echo $(cygpath -w $file)
  else
    echo $file
  fi
}


declare -r sbt_bin_dir="$(dirname "$(realpathish "$0")")"
declare -r sbt_home="$(dirname "$sbt_bin_dir")"

echoerr () {
  echo 1>&2 "$@"
}
vlog () {
  [[ $sbt_verbose || $sbt_debug ]] && echoerr "$@"
}
dlog () {
  [[ $sbt_debug ]] && echoerr "$@"
}

jar_file () {
  echo "$(cygwinpath "${sbt_home}/bin/sbt-launch.jar")"
}

jar_url () {
  local repo_base="$SBT_LAUNCH_REPO"
  if [[ $repo_base == "" ]]; then
    repo_base="https://repo1.maven.org/maven2"
  fi
  echo "$repo_base/org/scala-sbt/sbt-launch/$1/sbt-launch-$1.jar"
}

download_url () {
  local url="$1"
  local jar="$2"
  mkdir -p $(dirname "$jar") && {
    if command -v curl > /dev/null; then
      curl --silent -L "$url" --output "$jar"
    elif command -v wget > /dev/null; then
      wget --quiet -O "$jar" "$url"
    fi
  } && [[ -f "$jar" ]]
}

acquire_sbt_jar () {
  local launcher_sv="$1"
  if [[ "$launcher_sv" == "" ]]; then
    if [[ "$init_sbt_version" != "_to_be_replaced" ]]; then
      launcher_sv="$init_sbt_version"
    else
      launcher_sv="$builtin_sbt_version"
    fi
  fi
  download_jar="$HOME/.cache/sbt/boot/sbt-launch/$launcher_sv/sbt-launch-$launcher_sv.jar"
  if [[ -f "$download_jar" ]]; then
    sbt_jar="$download_jar"
  else
    sbt_url=$(jar_url "$launcher_sv")
    echoerr "downloading sbt launcher $launcher_sv"
    download_url "$sbt_url" "${download_jar}.temp"
    download_url "${sbt_url}.sha1" "${download_jar}.sha1"
    if command -v shasum > /dev/null; then
      if echo "$(cat "${download_jar}.sha1")  ${download_jar}.temp" | shasum -c - > /dev/null; then
        mv "${download_jar}.temp" "${download_jar}"
      else
        echoerr "failed to download launcher jar: $sbt_url (shasum mismatch)"
        exit 2
      fi
    else
      mv "${download_jar}.temp" "${download_jar}"
    fi
    if [[ -f "$download_jar" ]]; then
      sbt_jar="$download_jar"
    else
      echoerr "failed to download launcher jar: $sbt_url"
      exit 2
    fi
  fi
}

# execRunner should be called only once to give up control to java
execRunner () {
  # print the arguments one to a line, quoting any containing spaces
  [[ $sbt_verbose || $sbt_debug ]] && echo "# Executing command line:" && {
    for arg; do
      if printf "%s\n" "$arg" | grep -q ' '; then
        printf "\"%s\"\n" "$arg"
      else
        printf "%s\n" "$arg"
      fi
    done
    echo ""
  }

  if [[ "$CYGWIN_FLAG" == "true" ]]; then
    # In cygwin we loose the ability to re-hook stty if exec is used
    # https://github.com/sbt/sbt-launcher-package/issues/53
    "$@"
  else
    exec "$@"
  fi
}

addJava () {
  dlog "[addJava] arg = '$1'"
  java_args=( "${java_args[@]}" "$1" )
}
addSbt () {
  dlog "[addSbt] arg = '$1'"
  sbt_commands=( "${sbt_commands[@]}" "$1" )
}
addResidual () {
  dlog "[residual] arg = '$1'"
  residual_args=( "${residual_args[@]}" "$1" )
}
addDebugger () {
  addJava "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$1"
}

addMemory () {
  dlog "[addMemory] arg = '$1'"
  # evict memory related options
  local xs=("${java_args[@]}")
  java_args=()
  for i in "${xs[@]}"; do
    if ! [[ "${i}" == *-Xmx* ]] && ! [[ "${i}" == *-Xms* ]] && ! [[ "${i}" == *-Xss* ]] && ! [[ "${i}" == *-XX:MaxPermSize* ]] && ! [[ "${i}" == *-XX:MaxMetaspaceSize* ]] && ! [[ "${i}" == *-XX:ReservedCodeCacheSize* ]]; then
      java_args+=("${i}")
    fi
  done
  local ys=("${sbt_options[@]}")
  sbt_options=()
  for i in "${ys[@]}"; do
    if ! [[ "${i}" == *-Xmx* ]] && ! [[ "${i}" == *-Xms* ]] && ! [[ "${i}" == *-Xss* ]] && ! [[ "${i}" == *-XX:MaxPermSize* ]] && ! [[ "${i}" == *-XX:MaxMetaspaceSize* ]] && ! [[ "${i}" == *-XX:ReservedCodeCacheSize* ]]; then
      sbt_options+=("${i}")
    fi
  done
  # a ham-fisted attempt to move some memory settings in concert
  local mem=$1
  local codecache=$(( $mem / 8 ))
  (( $codecache > 128 )) || codecache=128
  (( $codecache < 512 )) || codecache=512
  local class_metadata_size=$(( $codecache * 2 ))
  if [[ -z $java_version ]]; then
      java_version=$(jdk_version)
  fi

  addJava "-Xms${mem}m"
  addJava "-Xmx${mem}m"
  addJava "-Xss4M"
  addJava "-XX:ReservedCodeCacheSize=${codecache}m"
  (( $java_version >= 8 )) || addJava "-XX:MaxPermSize=${class_metadata_size}m"
}

addDefaultMemory() {
  # if we detect any of these settings in ${JAVA_OPTS} or ${JAVA_TOOL_OPTIONS} we need to NOT output our settings.
  # The reason is the Xms/Xmx, if they don't line up, cause errors.
  if [[ "${java_args[@]}" == *-Xmx* ]] || \
     [[ "${java_args[@]}" == *-Xms* ]] || \
     [[ "${java_args[@]}" == *-Xss* ]] || \
     [[ "${java_args[@]}" == *-XX:+UseCGroupMemoryLimitForHeap* ]] || \
     [[ "${java_args[@]}" == *-XX:MaxRAM* ]] || \
     [[ "${java_args[@]}" == *-XX:InitialRAMPercentage* ]] || \
     [[ "${java_args[@]}" == *-XX:MaxRAMPercentage* ]] || \
     [[ "${java_args[@]}" == *-XX:MinRAMPercentage* ]]; then
    :
  elif [[ "${JAVA_TOOL_OPTIONS}" == *-Xmx* ]] || \
       [[ "${JAVA_TOOL_OPTIONS}" == *-Xms* ]] || \
       [[ "${JAVA_TOOL_OPTIONS}" == *-Xss* ]] || \
       [[ "${JAVA_TOOL_OPTIONS}" == *-XX:+UseCGroupMemoryLimitForHeap* ]] || \
       [[ "${JAVA_TOOL_OPTIONS}" == *-XX:MaxRAM* ]] || \
       [[ "${JAVA_TOOL_OPTIONS}" == *-XX:InitialRAMPercentage* ]] || \
       [[ "${JAVA_TOOL_OPTIONS}" == *-XX:MaxRAMPercentage* ]] || \
       [[ "${JAVA_TOOL_OPTIONS}" == *-XX:MinRAMPercentage* ]] ; then
    :
  elif [[ "${sbt_options[@]}" == *-Xmx* ]] || \
       [[ "${sbt_options[@]}" == *-Xms* ]] || \
       [[ "${sbt_options[@]}" == *-Xss* ]] || \
       [[ "${sbt_options[@]}" == *-XX:+UseCGroupMemoryLimitForHeap* ]] || \
       [[ "${sbt_options[@]}" == *-XX:MaxRAM* ]] || \
       [[ "${sbt_options[@]}" == *-XX:InitialRAMPercentage* ]] || \
       [[ "${sbt_options[@]}" == *-XX:MaxRAMPercentage* ]] || \
       [[ "${sbt_options[@]}" == *-XX:MinRAMPercentage* ]] ; then
    :
  else
    addMemory $sbt_default_mem
  fi
}

require_arg () {
  local type="$1"
  local opt="$2"
  local arg="$3"
  if [[ -z "$arg" ]] || [[ "${arg:0:1}" == "-" ]]; then
    echo "$opt requires <$type> argument"
    exit 1
  fi
}

is_function_defined() {
  declare -f "$1" > /dev/null
}

# parses JDK version from the -version output line.
# 8 for 1.8.0_nn, 9 for 9-ea etc, and "no_java" for undetected
jdk_version() {
  local result
  local lines=$("$java_cmd" -Xms32M -Xmx32M -version 2>&1 | tr '\r' '\n')
  local IFS=$'\n'
  for line in $lines; do
    if [[ (-z $result) && ($line = *"version \""*) ]]
    then
      local ver=$(echo $line | sed -e 's/.*version "\(.*\)"\(.*\)/\1/; 1q')
      # on macOS sed doesn't support '?'
      if [[ $ver = "1."* ]]
      then
        result=$(echo $ver | sed -e 's/1\.\([0-9]*\)\(.*\)/\1/; 1q')
      else
        result=$(echo $ver | sed -e 's/\([0-9]*\)\(.*\)/\1/; 1q')
      fi
    fi
  done
  if [[ -z $result ]]
  then
    result=no_java
  fi
  echo "$result"
}

# Extracts the preloaded directory from either -Dsbt.preloaded or -Dsbt.global.base
# properties by looking at:
#   - _JAVA_OPTIONS environment variable,
#   - SBT_OPTS environment variable,
#   - JAVA_OPTS environment variable and
#   - properties set by command-line options
# in that order. The last one will be chosen such that `sbt.preloaded` is
# always preferred over `sbt.global.base`.
getPreloaded() {
  local -a _java_options_array
  local -a sbt_opts_array
  local -a java_opts_array
  read -a _java_options_array <<< "$_JAVA_OPTIONS"
  read -a sbt_opts_array <<< "$SBT_OPTS"
  read -a java_opts_array <<< "$JAVA_OPTS"

  local args_to_check=(
    "${_java_options_array[@]}"
    "${sbt_opts_array[@]}"
    "${java_opts_array[@]}"
    "${java_args[@]}")
  local via_global_base="$HOME/.sbt/preloaded"
  local via_explicit=""

  for opt in "${args_to_check[@]}"; do
    if [[ "$opt" == -Dsbt.preloaded=* ]]; then
      via_explicit="${opt#-Dsbt.preloaded=}"
    elif [[ "$opt" == -Dsbt.global.base=* ]]; then
      via_global_base="${opt#-Dsbt.global.base=}/preloaded"
    fi
  done

  echo "${via_explicit:-${via_global_base}}"
}

syncPreloaded() {
  local source_preloaded="$sbt_home/lib/local-preloaded/"
  local target_preloaded="$(getPreloaded)"
  if [[ "$init_sbt_version" == "" ]]; then
    # FIXME: better $init_sbt_version detection
    init_sbt_version="$(ls -1 "$source_preloaded/org/scala-sbt/sbt/")"
  fi
  [[ -f "$target_preloaded/org/scala-sbt/sbt/$init_sbt_version/" ]] || {
    # lib/local-preloaded exists (This is optional)
    [[ -d "$source_preloaded" ]] && {
      command -v rsync >/dev/null 2>&1 && {
        mkdir -p "$target_preloaded"
        rsync --recursive --links --perms --times --ignore-existing "$source_preloaded" "$target_preloaded" || true
      }
    }
  }
}

# Detect that we have java installed.
checkJava() {
  local required_version="$1"
  # Now check to see if it's a good enough version
  local good_enough="$(expr $java_version ">=" $required_version)"
  if [[ "$java_version" == "" ]]; then
    echo
    echo "No Java Development Kit (JDK) installation was detected."
    echo Please go to http://www.oracle.com/technetwork/java/javase/downloads/ and download.
    echo
    exit 1
  elif [[ "$good_enough" != "1" ]]; then
    echo
    echo "The Java Development Kit (JDK) installation you have is not up to date."
    echo $script_name requires at least version $required_version+, you have
    echo version $java_version
    echo
    echo Please go to http://www.oracle.com/technetwork/java/javase/downloads/ and download
    echo a valid JDK and install before running $script_name.
    echo
    exit 1
  fi
}

copyRt() {
  local at_least_9="$(expr $java_version ">=" 9)"
  if [[ "$at_least_9" == "1" ]]; then
    # The grep for java9-rt-ext- matches the filename prefix printed in Export.java
    java9_ext=$("$java_cmd" "${sbt_options[@]}" "${java_args[@]}" \
      -jar "$sbt_jar" --rt-ext-dir | grep java9-rt-ext- | tr -d '\r')
    java9_rt=$(echo "$java9_ext/rt.jar")
    vlog "[copyRt] java9_rt = '$java9_rt'"
    if [[ ! -f "$java9_rt" ]]; then
      echo copying runtime jar...
      mkdir -p "$java9_ext"
      "$java_cmd" \
        "${sbt_options[@]}" \
        "${java_args[@]}" \
        -jar "$sbt_jar" \
        --export-rt \
        "${java9_rt}"
    fi
    addJava "-Dscala.ext.dirs=${java9_ext}"
  fi
}

run() {
  # Copy preloaded repo to user's preloaded directory
  syncPreloaded

  # no jar? download it.
  [[ -f "$sbt_jar" ]] || acquire_sbt_jar "$sbt_version" || {
    exit 1
  }

  # TODO - java check should be configurable...
  checkJava "6"

  # Java 9 support
  copyRt

  # If we're in cygwin, we should use the windows config, and terminal hacks
  if [[ "$CYGWIN_FLAG" == "true" ]]; then #"
    stty -icanon min 1 -echo > /dev/null 2>&1
    addJava "-Djline.terminal=jline.UnixTerminal"
    addJava "-Dsbt.cygwin=true"
  fi

  if [[ $print_sbt_version ]]; then
    execRunner "$java_cmd" -jar "$sbt_jar" "sbtVersion" | tail -1 | sed -e 's/\[info\]//g'
  elif [[ $print_sbt_script_version ]]; then
    echo "$init_sbt_version"
  elif [[ $print_version ]]; then
    execRunner "$java_cmd" -jar "$sbt_jar" "sbtVersion" | tail -1 | sed -e 's/\[info\]/sbt version in this project:/g'
    echo "sbt script version: $init_sbt_version"
  else
    # run sbt
    execRunner "$java_cmd" \
      "${java_args[@]}" \
      "${sbt_options[@]}" \
      -jar "$sbt_jar" \
      "${sbt_commands[@]}" \
      "${residual_args[@]}"
  fi

  exit_code=$?

  # Clean up the terminal from cygwin hacks.
  if [[ "$CYGWIN_FLAG" == "true" ]]; then #"
    stty icanon echo > /dev/null 2>&1
  fi
  exit $exit_code
}

declare -ra noshare_opts=(-Dsbt.global.base=project/.sbtboot -Dsbt.boot.directory=project/.boot -Dsbt.ivy.home=project/.ivy)
declare -r sbt_opts_file=".sbtopts"
declare -r build_props_file="$(pwd)/project/build.properties"
declare -r etc_sbt_opts_file="/etc/sbt/sbtopts"
# this allows /etc/sbt/sbtopts location to be changed
declare -r etc_file="${SBT_ETC_FILE:-$etc_sbt_opts_file}"
declare -r dist_sbt_opts_file="${sbt_home}/conf/sbtopts"
declare -r win_sbt_opts_file="${sbt_home}/conf/sbtconfig.txt"
declare sbt_jar="$(jar_file)"

usage() {
 cat <<EOM
Usage: `basename "$0"` [options]

  -h | --help         print this message
  -v | --verbose      this runner is chattier
  -V | --version      print sbt version information
  --numeric-version   print the numeric sbt version (sbt sbtVersion)
  --script-version    print the version of sbt script
  -d | --debug        set sbt log level to debug
  -debug-inc | --debug-inc
                      enable extra debugging for the incremental debugger
  --no-colors         disable ANSI color codes
  --color=auto|always|true|false|never
                      enable or disable ANSI color codes      (sbt 1.3 and above)
  --supershell=auto|always|true|false|never
                      enable or disable supershell            (sbt 1.3 and above)
  --traces            generate Trace Event report on shutdown (sbt 1.3 and above)
  --timings           display task timings report on shutdown
  --sbt-create        start sbt even if current directory contains no sbt project
  --sbt-dir   <path>  path to global settings/plugins directory (default: ~/.sbt)
  --sbt-boot  <path>  path to shared boot directory (default: ~/.sbt/boot in 0.11 series)
  --ivy       <path>  path to local Ivy repository (default: ~/.ivy2)
  --mem    <integer>  set memory options (default: $sbt_default_mem)
  --no-share          use all local caches; no sharing
  --no-global         uses global caches, but does not use global ~/.sbt directory.
  --jvm-debug <port>  Turn on JVM debugging, open at the given port.
  --batch             disable interactive mode

  # sbt version (default: from project/build.properties if present, else latest release)
  --sbt-version  <version>   use the specified version of sbt
  --sbt-jar      <path>      use the specified jar as the sbt launcher

  # java version (default: java from PATH, currently $(java -version 2>&1 | grep version))
  --java-home <path>         alternate JAVA_HOME

  # jvm options and output control
  JAVA_OPTS           environment variable, if unset uses "$default_java_opts"
  .jvmopts            if this file exists in the current directory, its contents
                      are appended to JAVA_OPTS
  SBT_OPTS            environment variable, if unset uses "$default_sbt_opts"
  .sbtopts            if this file exists in the current directory, its contents
                      are prepended to the runner args
  /etc/sbt/sbtopts    if this file exists, it is prepended to the runner args
  -Dkey=val           pass -Dkey=val directly to the java runtime
  -J-X                pass option -X directly to the java runtime
                      (-J is stripped)
  -S-X                add -X to sbt's scalacOptions (-S is stripped)

In the case of duplicated or conflicting options, the order above
shows precedence: JAVA_OPTS lowest, command line options highest.
EOM
}

process_my_args () {
  while [[ $# -gt 0 ]]; do
    case "$1" in
             -batch|--batch) exec </dev/null && shift ;; #>

   -sbt-create|--sbt-create) sbt_create=true && shift ;;

                        new) sbt_new=true && addResidual "$1" && shift ;;

                          *) addResidual "$1" && shift ;;
    esac
  done

  # Now, ensure sbt version is used.
  [[ "${sbt_version}XXX" != "XXX" ]] && addJava "-Dsbt.version=$sbt_version"

  # Confirm a user's intent if the current directory does not look like an sbt
  # top-level directory and neither the -sbt-create option nor the "new"
  # command was given.
  [[ -f ./build.sbt || -d ./project || -n "$sbt_create" || -n "$sbt_new" ]] || {
    echo "[warn] Neither build.sbt nor a 'project' directory in the current directory: $(pwd)"
    while true; do
      echo 'c) continue'
      echo 'q) quit'

      read -p '? ' || exit 1
      case "$REPLY" in
        c|C) break ;;
        q|Q) exit 1 ;;
      esac
    done
  }
}

## map over argument array. this is used to process both command line arguments and SBT_OPTS
map_args () {
  local options=()
  local commands=()
  while [[ $# -gt 0 ]]; do
    case "$1" in
     -no-colors|--no-colors) options=( "${options[@]}" "-Dsbt.log.noformat=true" ) && shift ;;
         -timings|--timings) options=( "${options[@]}" "-Dsbt.task.timings=true" "-Dsbt.task.timings.on.shutdown=true" ) && shift ;;
           -traces|--traces) options=( "${options[@]}" "-Dsbt.traces=true" ) && shift ;;
             --supershell=*) options=( "${options[@]}" "-Dsbt.supershell=${1:13}" ) && shift ;;
              -supershell=*) options=( "${options[@]}" "-Dsbt.supershell=${1:12}" ) && shift ;;
                  --color=*) options=( "${options[@]}" "-Dsbt.color=${1:8}" ) && shift ;;
                   -color=*) options=( "${options[@]}" "-Dsbt.color=${1:7}" ) && shift ;;
       -no-share|--no-share) options=( "${options[@]}" "${noshare_opts[@]}" ) && shift ;;
     -no-global|--no-global) options=( "${options[@]}" "-Dsbt.global.base=$(pwd)/project/.sbtboot" ) && shift ;;
                 -ivy|--ivy) require_arg path "$1" "$2" && options=( "${options[@]}" "-Dsbt.ivy.home=$2" ) && shift 2 ;;
       -sbt-boot|--sbt-boot) require_arg path "$1" "$2" && options=( "${options[@]}" "-Dsbt.boot.directory=$2" ) && shift 2 ;;
         -sbt-dir|--sbt-dir) require_arg path "$1" "$2" && options=( "${options[@]}" "-Dsbt.global.base=$2" ) && shift 2 ;;
             -debug|--debug) commands=( "${commands[@]}" "-debug" ) && shift ;;
     -debug-inc|--debug-inc) options=( "${options[@]}" "-Dxsbt.inc.debug=true" ) && shift ;;
                          *) options=( "${options[@]}" "$1" ) && shift ;;
    esac
  done
  declare -p options
  declare -p commands
}

process_args () {
  while [[ $# -gt 0 ]]; do
    case "$1" in
            -h|-help|--help) usage; exit 1 ;;
      -v|-verbose|--verbose) sbt_verbose=1 && shift ;;
      -V|-version|--version) print_version=1 && shift ;;
          --numeric-version) print_sbt_version=1 && shift ;;
           --script-version) print_sbt_script_version=1 && shift ;;
          -d|-debug|--debug) sbt_debug=1 && addSbt "-debug" && shift ;;
                   --client) use_sbtn=1 && shift ;;
                   --server) use_sbtn=0 && shift ;;

                 -mem|--mem) require_arg integer "$1" "$2" && addMemory "$2" && shift 2 ;;
     -jvm-debug|--jvm-debug) require_arg port "$1" "$2" && addDebugger $2 && shift 2 ;;
             -batch|--batch) exec </dev/null && shift ;;

         -sbt-jar|--sbt-jar) require_arg path "$1" "$2" && sbt_jar="$2" && shift 2 ;;
 -sbt-version|--sbt-version) require_arg version "$1" "$2" && sbt_version="$2" && shift 2 ;;
     -java-home|--java-home) require_arg path "$1" "$2" &&
                             java_cmd="$2/bin/java" &&
                             export JAVA_HOME="$2" &&
                             export JDK_HOME="$2" &&
                             export PATH="$2/bin:$PATH" &&
                             shift 2 ;;

                  "-D*"|-D*) addJava "$1" && shift ;;
                        -J*) addJava "${1:2}" && shift ;;
                          *) addResidual "$1" && shift ;;
    esac
  done

  is_function_defined process_my_args && {
    myargs=("${residual_args[@]}")
    residual_args=()
    process_my_args "${myargs[@]}"
  }
}

loadConfigFile() {
  # Make sure the last line is read even if it doesn't have a terminating \n
  cat "$1" | sed $'/^\#/d;s/\r$//' | while read -r line || [[ -n "$line" ]]; do
    eval echo $line
  done
}

loadPropFile() {
  while IFS='=' read -r k v; do
    if [[ "$k" == "sbt.version" ]]; then
      build_props_sbt_version="$v"
    fi
  done <<< "$(cat "$1" | sed $'/^\#/d;s/\r$//')"
}

detectNativeClient() {
  if [[ "$sbtn_command" != "" ]]; then
    :
  elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    [[ -f "${sbt_bin_dir}/sbtn-x86_64-pc-linux" ]] && sbtn_command="${sbt_bin_dir}/sbtn-x86_64-pc-linux"
  elif [[ "$OSTYPE" == "darwin"* ]]; then
    [[ -f "${sbt_bin_dir}/sbtn-x86_64-apple-darwin" ]] && sbtn_command="${sbt_bin_dir}/sbtn-x86_64-apple-darwin"
  elif [[ "$OSTYPE" == "cygwin" ]] || [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
    [[ -f "${sbt_bin_dir}/sbtn-x86_64-pc-win32.exe" ]] && sbtn_command="${sbt_bin_dir}/sbtn-x86_64-pc-win32.exe"
  elif [[ "$OSTYPE" == "freebsd"* ]]; then
    :
  else
    :
  fi
}

# Run native client if build.properties points to 1.4+ and has SBT_NATIVE_CLIENT
isRunNativeClient() {
  sbtV="$build_props_sbt_version"
  [[ "$sbtV" == "" ]] && sbtV="$init_sbt_version"
  [[ "$sbtV" == "" ]] && sbtV="0.0.0"
  sbtBinaryV_1=$(echo "$sbtV" | sed 's/^\([0-9]*\)\.\([0-9]*\).*$/\1/')
  sbtBinaryV_2=$(echo "$sbtV" | sed 's/^\([0-9]*\)\.\([0-9]*\).*$/\2/')
  if (( $sbtBinaryV_1 >= 2 )) || ( (( $sbtBinaryV_1 >= 1 )) && (( $sbtBinaryV_2 >= 4 )) ); then
    if [[ "$use_sbtn" == "1" ]] && [[ "$sbtn_command" != "" ]]; then
      echo "true"
    else
      echo "false"
    fi
  else
    echo "false"
  fi
}

runNativeClient() {
  vlog "[debug] running native client"
  for i in "${!original_args[@]}"; do
    if [[ "${original_args[i]}" = "--client" ]]; then
      unset 'original_args[i]'
    fi
  done
  sbt_script=$0
  sbt_script=${sbt_script/ /%20}
  execRunner "$sbtn_command" "--sbt-script=$sbt_script" "${original_args[@]}"
}

original_args=("$@")

# Here we pull in the default settings configuration.
[[ -f "$dist_sbt_opts_file" ]] && set -- $(loadConfigFile "$dist_sbt_opts_file") "$@"

# Here we pull in the global settings configuration.
[[ -f "$etc_file" ]] && set -- $(loadConfigFile "$etc_file") "$@"

# Pull in the project-level config file, if it exists.
[[ -f "$sbt_opts_file" ]] && set -- $(loadConfigFile "$sbt_opts_file") "$@"

# Pull in the project-level java config, if it exists.
[[ -f ".jvmopts" ]] && export JAVA_OPTS="$JAVA_OPTS $(loadConfigFile .jvmopts)"

# Pull in default JAVA_OPTS
[[ -z "${JAVA_OPTS// }" ]] && export JAVA_OPTS="$default_java_opts"

[[ -f "$build_props_file" ]] && loadPropFile "$build_props_file"

detectNativeClient

java_args=($JAVA_OPTS)
sbt_options0=(${SBT_OPTS:-$default_sbt_opts})
if [[ "$SBT_NATIVE_CLIENT" == "true" ]]; then
  use_sbtn=1
fi

# Split SBT_OPTS into options/commands
miniscript=$(map_args "${sbt_options0[@]}") && eval "${miniscript/options/sbt_options}" && \
eval "${miniscript/commands/sbt_additional_commands}"

# Combine command line options/commands and commands from SBT_OPTS
miniscript=$(map_args "$@") && eval "${miniscript/options/cli_options}" && eval "${miniscript/commands/cli_commands}"
args1=( "${cli_options[@]}" "${cli_commands[@]}" "${sbt_additional_commands[@]}" )

# process the combined args, then reset "$@" to the residuals
process_args "${args1[@]}"
vlog "[sbt_options] $(declare -p sbt_options)"

if [[ "$(isRunNativeClient)" == "true" ]]; then
  set -- "${residual_args[@]}"
  argumentCount=$#
  runNativeClient
else
  java_version="$(jdk_version)"
  vlog "[process_args] java_version = '$java_version'"
  addDefaultMemory
  set -- "${residual_args[@]}"
  argumentCount=$#
  run
fi

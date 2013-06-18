require "fileutils"
require "mortar/plugin"
require "mortar/command"
require "mortar/command/base"
require "bacon-bits/mortar/baconbits/lib"

# Mortar's library of reusable pigscripts and macros.
# 
class Mortar::Command::BaconBits < Mortar::Command::Base
  # baconbits
  #
  # List Bacon Bits pigscripts and controlscripts
  #
  def index
    validate_arguments!
    display "=== pigscripts"
    display Mortar::BaconBits::Lib.list_pigscripts
    display
    display "=== controlscripts"
    display Mortar::BaconBits::Lib.list_controlscripts
    display
  end

  # baconbits:run SCRIPT
  #
  # Run a script from Bacon Bits on a cluster.
  #
  # -c, --clusterid CLUSTERID   # Run job on an existing cluster with ID of CLUSTERID (optional)
  # -s, --clustersize NUMNODES  # Run job on a new cluster, with NUMNODES nodes (optional; must be >= 2 if provided)
  # -1, --singlejobcluster      # Stop the cluster after job completes.  (Default: false—-cluster can be used for other jobs, and will shut down after 1 hour of inactivity)
  # -2, --permanentcluster      # Don't automatically stop the cluster after it has been idle for an hour (Default: false--cluster will be shut down after 1 hour of inactivity)
  # -p, --parameter NAME=VALUE  # Set a pig parameter value in your script.
  # -f, --param-file PARAMFILE  # Load pig parameter values from a file.
  # -d, --donotnotify           # Don't send an email on job completion.  (Default: false--an email will be sent to you once the job completes)
  # -P, --project PROJECTNAME   # Use a project that is not checked out in the current directory.  Runs code from project's master branch in github rather than snapshotting local code.
  # -B, --branch BRANCHNAME     # Used with --project to specify a non-master branch
  #
  # Examples:
  #
  #    Lorem ipsum dolor sit amet
  #        $ nunc ubiquisquam redire
  def run
    script_name = shift_argument
    unless script_name
      error("Usage: mortar baconbits:run SCRIPT.\nMust specify SCRIPT.")
    end
    validate_arguments!

    # reconstructing command line args from options hash
    # there's probably a better way to do this
    args = [script_name] + options.flat_map do |k, v|
      v.kind_of?(Array) ? v.flat_map { |e| ["--" + k.to_s, e.to_s]  } : ["--" + k.to_s, v.to_s]
    end
    
    Mortar::BaconBits::Lib.ensure_project_registered(api)
    Mortar::BaconBits::Lib.run_command("run", args)
  end

  # baconbits:local_run SCRIPT
  #
  # Run a script from Bacon Bits in local mode.
  #
  # -p, --parameter NAME=VALUE  # Set a pig parameter value.
  # -f, --param-file PARAMFILE  # Load pig parameter values from a file.
  #
  # Examples:
  #
  #    Lorem ipsum dolor sit amet
  #        $ nunc ubiquisquam redire
  def local_run
    script_name = shift_argument
    unless script_name
      error("Usage: mortar baconbits:local_run SCRIPT.\nMust specify SCRIPT.")
    end
    validate_arguments!

    # reconstructing command line args from options hash
    # there's probably a better way to do this
    args = [script_name] + options.flat_map do |k, v|
      v.kind_of?(Array) ? v.flat_map { |e| ["--" + k.to_s, e.to_s]  } : ["--" + k.to_s, v.to_s]
    end

    Mortar::BaconBits::Lib.run_command("local:run", args)
  end

  # baconbits:use
  #
  # Install Bacon Bits components into a Mortar project so you can use them in your own code.
  #
  def use
    Mortar::BaconBits::Lib.ensure_dir_exists "vendor"
    Mortar::BaconBits::Lib.ensure_dir_exists "vendor/controlscripts"
    Mortar::BaconBits::Lib.ensure_dir_exists "vendor/pigscripts"
    Mortar::BaconBits::Lib.ensure_dir_exists "vendor/macros"

    bacon_bits_dir = Mortar::BaconBits::Lib.install_dir()
    FileUtils.cp_r("#{bacon_bits_dir}/controlscripts", "vendor")
    FileUtils.cp_r("#{bacon_bits_dir}/pigscripts", "vendor")
    FileUtils.cp_r("#{bacon_bits_dir}/macros", "vendor")
  end
end


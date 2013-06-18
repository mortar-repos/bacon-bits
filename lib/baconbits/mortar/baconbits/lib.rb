require "fileutils"
require "mortar/auth"
require "mortar/command"

module Mortar
  module BaconBits
    module Lib
      def self.install_dir()
        File.expand_path("../../../../..", __FILE__)
      end

      def self.list_pigscripts()
        Dir.glob("#{install_dir()}/pigscripts/*").map { |p| File.basename(p) }
      end

      def self.list_controlscripts()
        Dir.glob("#{install_dir()}/controlscripts/*").map { |p| File.basename(p) }.select { |f| f != "__init__.py" }
      end

      def self.run_in_install_dir(&block)
        curdir = Dir.pwd
        Dir.chdir(install_dir())
        yield
        Dir.chdir(curdir)
      end

      def self.ensure_project_registered(api)
        run_in_install_dir do
          unless File.exists? ".baconbits_registered_flag"
            project_name = Mortar::Auth.user_s3_safe + "-baconbits"
            projects = api.get_projects().body["projects"].collect { |x| x["name"] }
            if projects.include? project_name
              Mortar::Command::run("projects:set_remote", [project_name])
            else
              Mortar::Command::run("projects:register", [project_name])
            end
            FileUtils.touch(".baconbits_registered_flag")
          end
        end
      end

      def self.run_command(command, args)
        run_in_install_dir do
          Mortar::Command::run(command, args)
        end
      end

      def self.ensure_dir_exists(name)
        unless File.directory? name
          Dir.mkdir name
        end
      end
    end
  end
end

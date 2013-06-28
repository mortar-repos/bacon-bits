# init.rb
#
# This is the initializer plugin that tells mortar what it needs to load
# in order to run this plugin. Note, you'll want to do as much lazy loading as possible.
# this file will be run everytime the mortar command is called. A large init.rb file will
# cause the entire Mortar CLI tool to slow down.
#
require File.join(File.dirname(__FILE__), "bundle/bundler/setup")
require "baconbits/mortar/command/baconbits"

# Bacon Bits

Bacon Bits is a library of Pig macros and UDFs which implement useful algorithms for data science, currently focusing on matrix operations and graph algorithms. It is developed and maintained by [Mortar Data](http://www.mortardata.com/) and designed to be used with the [Mortar Development Framework](http://help.mortardata.com/tutorials/overview/getting_started_with_mortar).

## Installation

These installation instructions assume you have already installed the Mortar Development Framework.

To install Bacon Bits, run:

    mortar plugins:install git@github.com:mortardata/bacon-bits.git

This will add a command `mortar baconbits` to the mortar gem. You can use this to run some premade algorithms, such as Pagerank and a Graph Sampler, directly. See the documentation for individual scripts in `docs/scripts`.

To use Bacon Bits macros and UDFs in your own Mortar project, go to the root directory of the project and run:

    mortar baconbits:use

This will install all of the Bacon Bits components into the `vendor` directory of your Mortar project. They are automatically included in Pig's search path for resources, so you can reference them in your IMPORT and REGISTER statements just using the filename, for example `IMPORT 'matrix.pig';`. See the documentation for components in `docs/components`.

## Updating

To update Bacon Bits, run:

    mortar plugins:update baconbits

And then re-run `mortar baconbits:use` for the Mortar project you are working on.

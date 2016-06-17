# RDDs, DataFrames and Datasets in Apache Spark

This repo contains the source for my 2016 [Northeast Scala Symposium][] talk,
_RDDs, DataFrames and Datasets in Apache Spark_, which I updated (a little)
for Apache Spark 2.0 and gave again at a Philly Area Scala Enthusiasts (PHASE)
Meetup in June, 2016 (<http://www.meetup.com/scala-phase/events/229870987/>).

**Slides*: You can see the actual deck, in action, 
[here](http://scala-phase.org/talks/rdds-dataframes-datasets-2016-06-16).

**Video**: The talk at the Northeast Scala Symposium was recorded. The
video is [here](https://www.youtube.com/watch?v=pZQsDloGB4w)

The Git tag `nescala` captures the code and presentation as given at
the Northeast Scala Symposium.

The tag `phase` captures the code and presentation as given at the PHASE
Meetup.

The presentation is in [presentation](presentation). The demo notebooks
are in [demo](demo), in runnable source form. Also in [demo](demo) is a
file called `notebooks.dbc`, which can be loaded directly into Databricks.
Feel free to sign up for the free
[Databricks Community Edition](http://databricks.com/ce/) and try them yourself.

The presentation is built with [Reveal.js][], augmented with some custom
build code. To build the presentation, you can run `rake` from the top level.

The presentation will end up in `dist/index.html`.

## Preparing to build the slides

1. Install [NodeJS][] and `npm`.
2. Install the [LESS][] preprocessor: `npm install -g less`
3. Install Bower: `npm install -g bower`
4. Run `bower install` locally.
5. Make sure you have a version of Ruby 2 installed. (This stuff has been
   tested with 2.2.3.)
6. Install Bundler: `gem install bundler`
7. Use Bundler to install the required Ruby gems: `bundle install`

## Building the Slides

Once you've successfully completed preparation, building the slide deck
is as simple as:

    $ rake

Rake will build `dist/index.html`, a [Reveal.js][] slide show. Just
open the file in your browser, and away you go.

## Installing the slide show

If you want to install the slide show somewhere (e.g., a web server), copy
the _entire_ `dist` directory (presumably renaming it).

## Making PDFs

To create PDF versions of the slides, open the HTML slides in Chrome or
Chromium. Then, tack `?print-pdf` on the end of the URL, and print the result.
See the [Reveal.js][] documentation for details.

[Ruby]: http://www.ruby-lang.org/
[Rake]: http://rake.rubyforge.org/
[Bundler]: http://gembundler.com/
[LESS]: http://lesscss.org/
[Reveal.js]: https://github.com/hakimel/reveal.js
[NodeJS]: http://nodejs.org
[PHASE]: http://scala-phase.org
[Northeast Scala Symposium]: http://www.nescala.org

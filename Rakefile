# -*- ruby -*-

require 'tempfile'
require 'grizzled/fileutil/includer'
require 'nokogiri'
include Grizzled::FileUtil

# ----------------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------------

PRESO_DIR      = "presentation"
OUTPUT_DIR     = "dist"
HTML_SOURCES   = Dir.glob("#{PRESO_DIR}/*.html") +
                 Dir.glob("#{PRESO_DIR}/slides/**/*.html")
OUTPUT_SLIDES  = "#{OUTPUT_DIR}/index.html"
SLIDES_LIST    = "slide-list.tmp"
# Since SVG images are inlined, we don't want to copy them. We also
# don't want to copy iDraw sources.
IMAGES_TO_COPY = Dir.glob("#{PRESO_DIR}/images/*.{png,jpg}")

WATCHMAN = [
  'watchman-make',
  '--make', 'rake',
  '-p',
  'Rakefile', "#{PRESO_DIR}/slides/*.html", "#{PRESO_DIR}/images/*",
  "#{PRESO_DIR}/presentation.html", "#{PRESO_DIR}/*.less",
  "#{PRESO_DIR}/bower.json",
  '-t', 'build'
].join(" ")

# ----------------------------------------------------------------------------
# Tasks
# ----------------------------------------------------------------------------

task :x do
  print <<EOF
PRESO_DIR=#{PRESO_DIR}
OUTPUT_DIR=#{OUTPUT_DIR}
HTML_SOURCES=#{HTML_SOURCES}
OUTPUT_SLIDES=#{OUTPUT_SLIDES}
IMAGES_TO_COPY=#{IMAGES_TO_COPY}
EOF
end

task :default => :build
task :build => [OUTPUT_SLIDES]
task :dist => :build
task :html => OUTPUT_SLIDES

desc "Watch for changed files and rebuild. Requires watchman."
task :watch do
  # Assumes watchman is installed. http://facebook.github.com/watchman
  sh WATCHMAN
end

file OUTPUT_SLIDES => [:css, :js, :images, :svg] + HTML_SOURCES do
  puts "#{PRESO_DIR}/presentation.html => #{OUTPUT_DIR}/index.html"
  inc = Includer.new("#{PRESO_DIR}/presentation.html",
                     include_pattern: '^\s*%include\s"([^"]+)',
                     allow_glob: true)
  lines = inc.to_a
  File.open "#{OUTPUT_DIR}/index.html", "w" do |f|
    lines.each do |line|
      f.write(line)
    end
  end
end

task :js => ["#{PRESO_DIR}/bower_components/reveal.js",
             "#{PRESO_DIR}/bower_components/jquery",
             "#{PRESO_DIR}/js/highlightjs"] do
  mkdir_p "#{OUTPUT_DIR}/js"
  cp_r "#{PRESO_DIR}/bower_components/reveal.js", "#{OUTPUT_DIR}/js"
  cp_r "#{PRESO_DIR}/bower_components/jquery", "#{OUTPUT_DIR}/js"
  cp_r "#{PRESO_DIR}/js/highlightjs", "#{OUTPUT_DIR}/js"
end

task :imagedir do
  mkdir_p "#{OUTPUT_DIR}/images"
end

task :images => ["#{PRESO_DIR}/images/", :imagedir] do
  IMAGES_TO_COPY.each { |p| cp p, "#{OUTPUT_DIR}/images" }
end

SVG_FILES = Dir.glob("#{PRESO_DIR}/images/*.svg")
task :svg => SVG_FILES + ["#{PRESO_DIR}/images/", :imagedir] do
  # For each file, modify each <g> element with an "id" attribute that
  # starts with "Layer" so that the <g> element is a Reveal.js fragment.
  SVG_FILES.each do |svg|
    out = "#{OUTPUT_DIR}/images/#{File.basename(svg)}"
    puts "Animating layers in #{svg} to #{out}"
    augment_svg svg, out
  end
end

task :css => ["#{OUTPUT_DIR}/css/custom.css"]

file "#{OUTPUT_DIR}/css/custom.css" => ["#{PRESO_DIR}/custom.less"] do
  mkdir_p "#{OUTPUT_DIR}/css"
  sh 'sh', '-c', "lessc #{PRESO_DIR}/custom.less >#{OUTPUT_DIR}/css/custom.css"
end

task :clean do
  rm_rf OUTPUT_DIR
end

# ----------------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------------

# Augment an SVG in two ways:
#
# 1. Remove the "x", "y" and "viewBox" attributes from the <svg> element.
#
# 2. Find any layers and mark them with class="fragment", so that Reveal.js
#    will animate them. A layer is assumed to be an SVG group (<g> element)
#    with an ID. Tools like iDraw use <g> elements with "id" attributes to
#    mark layers.
#
#    a. As a special case, any layer that has "one-time" anywhere in its name
#       will be marked with the Reveal.js "current-visible" class, meaning it's
#       shown only once.
#
# Parameters:
#
# svg_file           - the path to the SVG image
# svg_out            - path to the output SVG image
# add_fragment_index - If true (the default), also add a
#                      data-fragment-index="n" attribute to each layer <g>
#                      element, to force the layers to show up in the order
#                      they appear in the image.
def augment_svg(svg_file, svg_out, add_fragment_index: true)
  def get_classes(element)
    attr = element.attribute('class')
    if attr
      Set.new(attr.value.split(/\s+/))
    else
      Set.new([])
    end
  end

  def set_classes(element, collection)
    if collection.empty?
      element.remove_attribute('class')
    else
      element['class'] = collection.to_a.join(" ")
    end
    element['class']
  end

  def add_class(element, class_name)
    classes = get_classes(element)
    classes.add(class_name)
    set_classes(element, classes)
  end

  def remove_class(element, class_name)
    classes = get_classes(element)
    classes.delete(class_name)
    set_classes(element, classes)
  end

  def add_fragment(element)
    add_class(element, 'fragment')
  end

  def remove_fragment(g)
    remove_class(g, 'fragment')
    g.remove_attribute('data-fragment-index')
  end

  image = File.open(svg_file) { |f| Nokogiri::XML(f) }
  image.remove_namespaces!
  layers = image.xpath("//g[@id]")
  if layers.length == 0
    # Nothing to do
  elsif layers.length == 1
    # There's only one layer. No sense causing incremental display.
    # Remove any existing fragments.
    puts "Image #{svg_file} is a single-layer image. No animation."
    g = layers[0]
    remove_fragment(g)
  else
    layers.each_with_index do |g, i|
      # Don't mark the first layer; that should show up when the slide
      # shows up.
      if i == 0
        remove_fragment(g)
      else
        add_fragment(g)
        if add_fragment_index
          g['data-fragment-index'] = i.to_s
        end
      end

      id = g.attribute('id')
      if id.value.include?('one-time')
        add_class(g, 'current-visible')
      end

    end
  end

  %w{x y viewBox}.each { |attr| image.root.delete(attr) }

  File.open svg_out, "w" do |f|
    f.write(image.to_xml)
  end
end

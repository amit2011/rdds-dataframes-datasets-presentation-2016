$(document).ready(function() {
  // Find all <pre> <code> blocks, get the contents, and remove leading
  // and trailing blank lines.

  $("pre").children("code").each(function() {
    var html = $(this).html();
    var lines = html.split("\n");
    var html2 = _.filter(lines, function(line) {
      return line.trim().length > 0;
    });

    $(this).html(_.join(html2, "\n"));
  });


  // Initialize Reveal.js
  Reveal.initialize({
    slideNumber:  true,
    progress:     false,
    center:       false,
    history:      true,
    transition:   'slide',
    controls:     false,
    width:        2560,
    height:       1600,
    // Plugins
    dependencies: [
      {
        src:      'js/highlightjs/highlight.pack.js',
        async:    true,
        callback: function() { hljs.initHighlightingOnLoad(); }
      },
      {
        src:   'js/reveal.js/plugin/zoom-js/zoom.js',
        async: true
      },
      {
        src:   'js/reveal.js/plugin/notes/notes.js',
        async: true
      },
      { src: 'js/reveal.js/plugin/markdown/marked.js',
        condition: function() {
          return !!document.querySelector( '[data-markdown]' );
        }
      },
      { src: 'js/reveal.js/plugin/markdown/markdown.js',
        condition: function() {
          return !!document.querySelector( '[data-markdown]' );
        }
      }
    ]
  });

  Reveal.addEventListener('markdown-slide', function(e) {
    // Alternative to adding ugly HTML for Markdown elements:
    // Mark slide section with data-state="markdown-slide", and
    // this code will add fragment classes to all <li>
    // elements in the slide when the slide is shown.
    $("section.present li").addClass("fragment");
  }, false);

  function handleSlideChange(index, slide) {
    function setBodyClass(className) {
      var body = $("body");
      body.removeClass(); // remove all classes
      body.addClass(className);
    }

    // For some reason, the slide isn't always defined (e.g., on a
    // reload). So, we'll assume that any slide greater than 2 is a normal
    // slide, if the slide itself isn't defined.
    if (! slide) {
      switch (index) {
        case 0:
          slideType = "initial-slide";
          break;
        case 1:
          slideType = "title-slide";
          break;
        default:
          slideType = "normal-slide";
      }
    }
    else {
      slideType = $(slide).data("type");
    }

    switch (slideType) {
      case "initial-slide":
        setBodyClass("initial-slide");
        break;
      case "title-slide":
        setBodyClass("title-slide");
        break;
      default:
        setBodyClass("normal-slide");
        break;
    }
  }

  Reveal.addEventListener('slidechanged', function(e) {
    // Handle the initial and title slides differently.
    handleSlideChange(e.indexh, e.currentSlide);
  });

  var indices = Reveal.getIndices();
  var h;
  if (indices)
    h = indices.h || 0;
  else
    h = 0;

  handleSlideChange(h, Reveal.getCurrentSlide());
});

jQuery(document).ready(function($) {


    // Sticky Footer
    var $sticky = $('.sticky-links'),
      $footer = $('.site-footer'),
      $stickyMobile = $('.mobile-sticky-btn'),
      $document = $(document);
    function checkOffset() {
      if ($document.scrollTop() + window.innerHeight <= $footer.offset().top) {
        $sticky.removeClass('sticky-stuck');
      } else  {
        $sticky.addClass('sticky-stuck');
      }
    }
    function checkOffsetMobile() {
      if ($document.scrollTop() + window.innerHeight <= $footer.offset().top) {
        $stickyMobile.removeClass('sticky-stuck');
      } else  {
        $stickyMobile.addClass('sticky-stuck');
      }
    }
    checkOffset();
    checkOffsetMobile();
    $(document).scroll(checkOffset);
    $(window).resize(checkOffset);
    $(document).scroll(checkOffsetMobile);
    $(window).resize(checkOffsetMobile);

});

jQuery(window).scroll(function(){

  if(jQuery(this).scrollTop()>=55){
    jQuery('.site-header').addClass('sticky');
  }
  else {
    jQuery('.site-header').removeClass('sticky');
  }
});

jQuery(document).ready(function() {
    jQuery('.hamburger').click(function(e) {
        jQuery(this).toggleClass('sftoggle');
        jQuery('.left-sidebar').toggleClass('sftoggle');
 
        e.preventDefault();
    });
});
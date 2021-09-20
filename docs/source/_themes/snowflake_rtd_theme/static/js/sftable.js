jQuery(document).ready(function() {

    jQuery('td').filter(function() {
      return $(this).text().match(/\u2714/); /*regex match the unicode checkmark character */
    }).addClass('tdCheckmark')
});

jQuery(document).ready(function() {

    jQuery('td').filter(function() {
      return $(this).text().match(/\u274C/); /*regex match the unicode crossmark (red x) character */
    }).addClass('tdCheckmark')
});




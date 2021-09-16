// Script to append the current page URL to the mailto: link for doc-feedback.
window.onload = function() {
  var linkId = 'doc-feedback-link';
  var docFeedbackLink = document.getElementById(linkId);
  if ((docFeedbackLink !== null) && (typeof docFeedbackLink.href !== 'undefined' && docFeedbackLink.href.length > 0)) {
    var mailtoParams = '?subject=' + encodeURIComponent('Feedback on ' + document.location.href) + '&body=' + encodeURIComponent('Please suggest feedback on ' + document.location.href) + ':';
    docFeedbackLink.href += mailtoParams;
  }
};

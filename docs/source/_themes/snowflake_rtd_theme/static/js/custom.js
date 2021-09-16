
<script type="text/javascript">

document.body.onload = addElement;

function addElement () { 
  // create a new div element 
  var newDiv = document.createElement("div"); 
  // and give it some content 
  var newContent = document.createTextNode("blah blah blah"); 
  // add the text node to the newly created div
  newDiv.appendChild(newContent);  

  // add the newly created element and its content into the DOM 
  var currentDiv = document.getElementsByClassName("highlight"); 
  document.body.insertBefore(newDiv, currentDiv); 
}

</script>
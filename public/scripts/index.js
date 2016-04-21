function myFunction() {
    $('#upload').click();
}
function scroll(){
  $('#second').css('display', 'block');
  if(!$(window).scrollTop()) {
    $('html, body').animate({scrollTop: $("#end").offset().top}, 1000);
  }
  else{
    //$('html, body').animate({scrollTop: 0}, 500);
     $('html, body').animate({scrollTop: $("#end").offset().top}, 1000);
  }
}
function editor(){
  scroll();
  $('#uploader').css('display', 'none');
  $('#editor').css('display', 'block');
  $('#file_title').text("Input Python Code");
  window.state = "editor";
}

function uploader(){
  scroll();
  $('#editor').css('display', 'none');
  $('#uploader').css('display', 'block');
  $('#file_title').text("File Upload");

  window.state = "uploader";
}

function submit(){
  var data = myCodeMirror.getValue();
  $('#input_file_data').val(data);
  $('#input_submit').click();
}


function handleFileSelect()
  { 
    if (!window.File || !window.FileReader || !window.FileList || !window.Blob) {
      alert('The File APIs are not fully supported in this browser.');
      return;
    }   

      file = window.file_uploaded;
      fr = new FileReader();
      fr.onload = receivedText;
      fr.readAsText(file);
  }

  function receivedText() {
    myCodeMirror.setValue(fr.result);
  }    


var myCodeMirror;
$( document ).ready(function() {
    myCodeMirror = CodeMirror(document.getElementById("editor"), {
      value: "",
      mode:  "python"
    });

Dropzone.options.mainDropzone = {
  accept: function(file, done) {
    $('#uploader').css('z-index', '0');
    window.file_uploaded = file;
    handleFileSelect()
  },
  autoProcessQueue: false,
  uploadprogress: function(file, progress, bytesSent) {
    // Display the progress
  }
  };
});







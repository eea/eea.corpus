jQuery(function($){
  var $reloadBtn = $(".phrase-component-form .button-reload");

  $.each($reloadBtn, function(){
    var phashId = $(this).parent().find("._phash_id").val();
    var url = "/phase-model-status/" + phashId;
    console.log("you clicked me", url);
  });

});

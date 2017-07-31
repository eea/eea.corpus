jQuery(document).ready(function(){
  $('.collapsed-form .collapse-button').click(function(){
    $('i', this).toggleClass('glyphicon-chevron-left').toggleClass('glyphicon-chevron-down');
  });
});

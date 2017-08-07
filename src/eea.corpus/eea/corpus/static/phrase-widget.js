jQuery(document).ready(function(){
  console.log('ready from phrases');
  $('.phrase-component-form .button-reload').on('click', function() {
    var phash_id = $(this).parent().find('._phash_id').val();
    console.log('you clicked me', phash_id);
    return false;
  });
});

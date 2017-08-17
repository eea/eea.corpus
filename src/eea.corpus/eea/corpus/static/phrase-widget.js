function setButtonStatus($el, ok){
  $el.toggleClass('disabled', !ok);
  $el.toggleClass('btn-success', ok);
  $el.toggleClass('btn-warning', !ok);
  $el.find('span').toggleClass('glyph-spinner', !ok);
}

function setPanelStatus($el, ok) {
  $el.toggleClass('panel-success', ok);
  $el.toggleClass('panel-danger', !ok);
}

function checkPhraseModelStatus() {
  var $reloadBtn = $('.phrase-component-form .button-reload');

  $.each($reloadBtn, function(){
    var $btn = $(this);
    var phashId = $btn.parent().find('._phash_id').val();
    var url = '/phrase-model-status/' + phashId;
    $.getJSON(url, {}, function(data) {
      var ok = data['status'] === 'OK';
      var $panel = $btn.parents('.phrase-component-form');

      setButtonStatus($btn, ok);
      setPanelStatus($panel, ok);

      if (ok) {
        clearInterval(window._PMI);
      }

    });
  });
}

jQuery(function(){
  window._PMI = window.setInterval(checkPhraseModelStatus, 2000);
  checkPhraseModelStatus();
});

function aa(){
  
  //función para probar si encuentra el código (tiene que correr un "correo"(usuario) que este en el hilo de correos)
  var number = 0;
  var found_code = 0;
  var email = GmailApp.search('subject:1940669169763');
  Logger.log(email);
  var messages = email[email.length-1].getMessages();
  for (var z = 1; z<messages.length;z++){
      var sender = messages[z].getFrom();
      
      if(sender.split('@')[1].replace('>','') == 'booboo.eu'){
        var body = messages[z].getPlainBody();
        var results = findcode(body);
        number = results[0];
        found_code = results[1];
        Logger.log(found_code);
        Logger.log(number);
        Logger.log(number.length == 16 || number.length == 14);
      }
  }

}

function reademail(id_order,updated_at) {
  
 //esta funcion devuelve un 0 si no ha habido respuesta, devuelve un 1 si no ha encontrado el codigo y devuelve el codigo del envio si lo encuentra
  
  var email = GmailApp.search('subject:'+id_order+'');
  for(var j=0; j<email.length;j++){
    var messages = email[j].getMessages();
    var number = 0;
    var found_code = 0;
    var today = new Date();
    var today_day = today.getDate();
    var week_day = parseInt(today.getDay());
    var created_day = parseInt(String(updated_at).substring(8,10));
    
    //debe haber mejores maneras de calcular dias que han pasado
    var days_passed = Math.abs(today_day-created_day);
    
    var parameters = {
      isValid: true,
      content: 'some string',
      timestamp: new Date()
    };
    
    console.log({message: 'Days passed: '+String(days_passed), initialData: parameters});
    
    //si solo hay un correo les enviamos un recordatorio
    if((messages.length==1) && (days_passed > 3) && (week_day != 0) && (week_day != 6)){
      messages[0].replyAll('Buenos días, no hemos recibido respuesta de este pedido, Muchas Gracias, un saludo');
      number=1;
      return number
    }
    
    
    else if(messages.length==1){
      //devuleve 0 si no ha habido respuesta
      return number
    }
    
    else{
      //busca en todos los mensajes del hilo de correos
      for (var z = 1; z<messages.length;z++){
        //mira a ver si el que envia el correo es booboo
        var sender = messages[z].getFrom();
        if(sender.split('@')[1].replace('>','') == 'booboo.eu'){
          
          //coge el texto del mensaje
          var body = messages[z].getPlainBody();
          
          //busca un codigo de 14 o 16 digitos
          var results = findcode(body);
          
          //si hay respuesta pero no lo encuentra nos devuelve un 1
          number = results[0];
          found_code = results[1];
          
          //si lo encuentras da las gracias y devuelve el numero
          if(found_code){
            messages[z].replyAll('Recibido\nMillones de Gracias por todo, sois geniales');
            return number
          }
          
        }
        
      }
      
    }
  }
  //devuelve el 1 si no lo has encontrado
  return number
}

function findcode(body){
  
  //busca un codigo de 14 o de 16 cifras en el cuerpo del correo
  
  var number = 1;
  var found_code = 0;
  var all = body.split('\n');
  var words = [];
  
  for(var i=0; i<all.length;i++){
    var line=all[i].split(' ');
    if(line.length >0){
      for(var j=0;j<line.length;j++){
        words.push(line[j]);
      }
    }
  }
  
  var reg=/\d{16}/g;
  var reg2=/\d{14}/g;
  for(var i=0; i<words.length;i++){
    var word=words[i];
    word = word.replace(' ','').replace(' ','').replace('\n','').replace('.','');
    
    //Logger.log(word); //for testing
    //Logger.log(word.length);
    if(word.length == 17){
      if(reg.test(word)){
        number = word.match(reg);
        number = number[0];
        found_code=1;
      }
    }
    else if(word.length == 15){
      if(reg2.test(word)){
        number = word.match(reg2);
        number = number[0];
        found_code=1;
      }
    }
  }
  return [number,found_code]
}

function getopenordersandclose(){
  var file = DriveApp.getFileById('1QxKl5ec3cVKAkMrs2Msn2spIwaJgGmFZ');
  var content = file.getBlob().getDataAsString();
  var json_file = JSON.parse(content);
  var api_key = json_file['API_KEY'];
  var api_pass = json_file['PASSWORD'];
  var store_url = 'https://'+json_file['base_url']+'/api/2019-04';
  var encoded =  Utilities.base64Encode(api_key + ":" + api_pass);
  
  var headers = {
    "Content-Type" : "application/json",
    "Authorization": "Basic " + encoded
  };
    

  var params = {
    "method" : "GET",
    "headers" : headers,
    "contentType" : "application/json",
    'followRedirects' : false
  };
  
  var apiCall = function(endpoint){
    var apiResponseMembers = UrlFetchApp.fetch(store_url+endpoint,params);
    var json = JSON.parse(apiResponseMembers);
    return json
  }
  //var call = apiCall('/orders.json?status=any&ids=1193871310947');
  var call = apiCall('/orders.json?status=open');
  var orders = call['orders'];
  var ts_open=sendslackmessage_1('opened at this day');
  var ts_close=sendslackmessage_1('closed today');
  for(var i=0;i<orders.length;i++){
    var order = orders[i];
    var id_order = String(order['id']);
    var line_items = order['line_items'];
    var updated_at=order['updated_at'];
    var name = order['billing_address']['first_name']+' '+order['billing_address']['last_name']
    var number_id = reademail(id_order,updated_at);
    
    Logger.log(number_id);
    
    //no ha habido ninguna respuesta de su parte
    if(number_id == 0){
      sendslackmessage_3(ts_open,name,updated_at,id_order,number_id);
      update_pedidos_nikarit(id_order,"sin respuesta");
      continue;
    }
    //ha habido respuesta pero no he encontrado el codigo
    else if(number_id == 1){
      sendslackmessage_3(ts_open,name,updated_at,id_order,number_id);
      update_pedidos_nikarit(id_order,"con respuesta, numero no encontrado");
      continue;
    }
    //encuentra el codigo de 14 o 16 digitos
    else if(number_id.length == 16 || number_id.length == 14){
      post_fullfilment(number_id,line_items,id_order)
      sendslackmessage_2(ts_close,name,id_order,updated_at,number_id);
      update_pedidos_nikarit(id_order,number_id);
      continue;
    }
    //otra posibilidad, fuera de mi conocimiento
    else {
      sendslackmessage_3(ts_open,name,updated_at,id_order,number_id);
      update_pedidos_nikarit(id_order,"undefined");
      continue;
    }

  }
  
  var parameters = {
      isValid: true,
      content: 'some string',
      timestamp: new Date()
  };
  console.log({message: 'Number:'+String(number_id), initialData: parameters});
  
}

function post_fullfilment(number_id,line_items,id_order) {
  
  var file = DriveApp.getFileById('1QxKl5ec3cVKAkMrs2Msn2spIwaJgGmFZ');
  var content = file.getBlob().getDataAsString();
  var json_file = JSON.parse(content);
  var api_key = json_file['API_KEY'];
  var api_pass = json_file['PASSWORD'];
  var store_url = 'https://'+json_file['base_url']+'/api/2019-04';
  var encoded =  Utilities.base64Encode(api_key + ":" + api_pass);
  
  var headers = {
    "Content-Type" : "application/json",
    "Authorization": "Basic " + encoded
  };
  
  var data = {
    "fulfillment": {
      "location_id": 31056822371,
      "tracking_number": number_id,
      "tracking_url": 'https://s.correosexpress.com/SeguimientoSinCP/search',
      "tracking_company": 'Other',
      "line_items": line_items,
      "notify_customer": true
      
    }
  }
  var payload = JSON.stringify(data);
  
  var params = {
    "method" : "POST",
    "headers" : headers,
    "contentType" : "application/json",
    'followRedirects' : false,
    "payload" : payload
    //"muteHttpExceptions": true
  };
  
  var apiCall = function(endpoint){
    var apiResponseMembers = UrlFetchApp.fetch(store_url+endpoint,params);
    var json = JSON.parse(apiResponseMembers);
    return json
  }
  var call = apiCall('/orders/'+String(id_order)+'/fulfillments.json');
}

function createTimeDrivenTriggers() {
  
  ScriptApp.newTrigger('getopenordersandclose')
  .timeBased().everyDays(1).atHour(8).create();
}

function sendslackmessage_1(type){
  var pass_sheet = SpreadsheetApp.openById("1ILSXJ9m-Qers5ljEILs6Quyl-PQ3FWzCrhL3ossEipY");
  var tab_api = pass_sheet.getSheetByName("APIs");
  var nikarit_key= String(tab_api.getSheetValues(4, 3, 1, 1));
  
  var slack_url = "https://slack.com/api/chat.postMessage";
  
  var API_KEY=nikarit_key;
  
  var today = Utilities.formatDate(new Date(), "GMT+1", "dd/MM/yyyy");
  
  var data = {
    "channel": "CMX8EFENL",
    "text":  String(today)+'\n'+'Shippings '+type}
  
  var options = {
    "headers": {"authorization": 'Bearer '+API_KEY},
    'contentType': 'application/json',
    'payload' : JSON.stringify(data)
  };
  var response=UrlFetchApp.fetch(slack_url, options);
  var responsetext = JSON.parse(response.getContentText());
  
  return responsetext.ts

}

function sendslackmessage_2(ts,client,id,updated_at,number_id){
  var pass_sheet = SpreadsheetApp.openById("1ILSXJ9m-Qers5ljEILs6Quyl-PQ3FWzCrhL3ossEipY");
  var tab_api = pass_sheet.getSheetByName("APIs");
  var nikarit_key= String(tab_api.getSheetValues(4, 3, 1, 1));
  
  var slack_url = "https://slack.com/api/chat.postMessage";
  
  var API_KEY=nikarit_key;
  
  
   var data2 = {
    "channel": "CMX8EFENL",
    "thread_ts": ts,
     "text": 'Client: '+client+'\nID: '+id+'\nCreated at: '+ updated_at+'\nNº de Seguimiento: '+number_id
  };
  var options2 = {
    "headers": {"authorization": 'Bearer '+API_KEY},
    'contentType': 'application/json',
    'payload' : JSON.stringify(data2)
  };
  
  UrlFetchApp.fetch(slack_url, options2);

}

function sendslackmessage_3(ts,client,updated_at,id,number_id){
  var pass_sheet = SpreadsheetApp.openById("1ILSXJ9m-Qers5ljEILs6Quyl-PQ3FWzCrhL3ossEipY");
  var tab_api = pass_sheet.getSheetByName("APIs");
  var nikarit_key= String(tab_api.getSheetValues(4, 3, 1, 1));
  
  var slack_url = "https://slack.com/api/chat.postMessage";
  
  var API_KEY=nikarit_key;
  
  
   var data2 = {
    "channel": "CMX8EFENL",
    "thread_ts": ts,
     "text": 'Client: '+client+'\nID: '+id+'\nCreated at: '+ updated_at +'\n Sent email: '+number_id
  };
  var options2 = {
    "headers": {"authorization": 'Bearer '+API_KEY},
    'contentType': 'application/json',
    'payload' : JSON.stringify(data2)
  };
  
  UrlFetchApp.fetch(slack_url, options2);

}

function update_pedidos_nikarit(id_order,number_id){

  var ssheet = SpreadsheetApp.openById('1TfpGOFrEoNi8VHZFQuN2yb4eUWQ22K5NJSyzw8i1tKM');
  var todos = ssheet.getSheetByName('Todos');
  var last_row = todos.getLastRow();
  var ids = todos.getSheetValues(2, 14, last_row-1, 4);
  
  for(var i =0; i<last_row-1;i++){
    if(ids[i][0] == id_order){
      todos.getRange(i+2, 17).setValue(number_id);
    }
  }
  
}
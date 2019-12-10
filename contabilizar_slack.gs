//this is a function that fires when the webapp receives a GET request
function doGet(e) {
  //Test get request
  return HtmlService.createHtmlOutput("request received");
}

//this is a function that fires when the webapp receives a POST request
function doPost(e) {
  
  //get data and process data -> convert to dict
  var myData = JSON.parse(e.postData.contents);
  

  //custumer data from the dict made (name and surname)
  var customer = String(myData.customer.first_name) +' ' +String(myData.customer.last_name);
  
  //get today date
  var timestamp = new Date();
 
  
  //initiate the variables
  var manteca = 0;
  var crema = 0;
  var balsamo= 0;
  
  //initiate what will be in the message
  var fields = [];
  var bolsas = 0;
  //loop for every item in the order
  for(var i=0; i<myData.line_items.length;i++){
    var quantity = myData.line_items[i].quantity;
    
    //conditionals for every product reference
    if(String(myData.line_items[i].name).substring(0, 2).equals('02')){
      manteca+=quantity;
    }
    if(String(myData.line_items[i].name).substring(0, 2).equals('04')){
      crema+=quantity;
    }
    if(String(myData.line_items[i].name).substring(0, 2).equals('03')){
      balsamo+=quantity;
    }
    if(String(myData.line_items[i].name).substring(0, 2).equals('01')){
      manteca+=quantity;
      balsamo+=quantity;
      crema+=quantity;
      bolsas+=quantity;
    }
    if(String(myData.line_items[i].name).substring(0, 2).equals('05')){
      crema+=quantity;
      balsamo+=quantity;
    }
    if(String(myData.line_items[i].name).substring(0, 2).equals('06')){
      bolsas+=quantity;
    }
    
    //fields of the message
    fields.push( {
      "title": 'Item '+String(i+1)+':',
      "value": String(myData.line_items[i].name) +': '+String(quantity),
      "short": false
      
    });
   
  }
  
  var shiping_info = myData.shipping_address;
  var poblacion = shiping_info.city;
  var billingaddress = shiping_info.address1;
  var phone = shiping_info.phone;
  var zip = shiping_info.zip;
  var pais = shiping_info.country_code;
  var total_price = myData.total_price;
  var email = myData.email;
  
  var orderid = myData.id;
  
  var sex = sex_cal(myData.customer.first_name.replace(/ /g, ""));
  
  var n_pedidos_ant = count_pedidos(email);
  
  var bool_repeat=readId(orderid);
  
  if(bool_repeat){
    return
  }
  
  // data para contabilizar
  
  var data_contabilidad = {
    "fecha":myData.created_at,
    "ingresos":total_price,
    "orderid":orderid,
    "gateway":myData.gateway
    
  };
  contabilizar(data_contabilidad);
  
  var data_bq="("+String(orderid)+",'"+String(myData.created_at)+"',"+String(total_price)+",'"+customer+"','"+sex+"','"+email+"',"+
    String(manteca)+","+String(crema)+","+String(balsamo)+",'"+poblacion+"',"+
      myData.shipping_address.latitude+","+myData.shipping_address.longitude+")"
    
  insertqueryDataTable(data_bq);
  
  //enviar info a slack
  //Abrir tabla con contraseñas
  var ssheet = SpreadsheetApp.openById('ID');
  var sheet = ssheet.getSheetByName('Sheet1');
  var API_KEY=sheet.getSheetValues(2, 3, 1, 1);
  
  //Send message to slack
  var slack_url = "https://slack.com/api/chat.postMessage";
  
  fields.push( {
    "title": 'Mantecas:     Bálsamos:       Crema:',
    "value": String(manteca) +' item(s)'+'          '+ String(balsamo) +' item(s)'+'           '+ String(crema) +' item(s)',
    "short": false
    
  });
   fields.push( {
    "title": 'Bolsas Africanas:',
    "value": String(bolsas) +' item(s)',
    "short": false
    
  });
  fields.push( {
    "title": 'Ciudad:',
    "value": String(poblacion),
    "short": false
    
  });
  fields.push( {
    "title": 'Sexo:',
    "value": String(sex),
    "short": false
    
  });
  
    fields.push( {
      "title": 'Pedidos anteriores de '+email+':',
      "value": String(n_pedidos_ant)+' pedidos',
      "short": false
  });
  
  
  
  
  var attachments = [{
    "fallback": "Data of the new order",
    "color": "#563528",
    "author_name": "Pedido nº "+String(orderid),
    "author_link": "https://nikarit.myshopify.com/admin/orders/"+String(orderid),
    "fields": fields,
    "footer": "5danalfaro",
    "footer_icon": "https://st-listas.20minutos.es/images/2019-06/440239/5476511_640px.jpg"
  }];
  var data = {
    "channel": "CJNRWH0UD",
    "text": '*NUEVA VENTA ONLINE*\n*Cliente*: '+String(customer)+'\n*Ingresos*: '+String(total_price)+'€',
    'attachments': attachments
    
  };
  
  
  var options = {
    "headers": {"authorization": 'Bearer '+API_KEY},
    'contentType': 'application/json',
    'payload' : JSON.stringify(data),
   
  };
  
  var response = UrlFetchApp.fetch(slack_url, options);
  var responsetext = JSON.parse(response.getContentText());
  
  var data2 = {
    "channel": "CJNRWH0UD",
    "thread_ts": responsetext.ts,
    "text": '¡Viva a Cristo Rey, quien nos enseña a dar la vida para dar vida!'
  };
  var options2 = {
    "headers": {"authorization": 'Bearer '+API_KEY},
    'contentType': 'application/json',
    'payload' : JSON.stringify(data2)
  };
  
  UrlFetchApp.fetch(slack_url, options2);
}


function update_stock(number_id,line_items,id_order) {
  
  var file = DriveApp.getFileById('ID');
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
  var call = apiCall(String(id_order)+'/inventory_levels/set.json');
}

function contabilizar(data){
  var ss = SpreadsheetApp.openById('ID');
  var diario = ss.getSheetByName('Diario2019');
  
  var fechas = new Date();
  var fecha = new Date(String(data.fecha));
  var ingresos = data.ingresos;
  var orderid = data.orderid;
  var day = fecha.getDate();
  var month = fecha.getMonth()+1;
  var year = fecha.getYear();
  var date = String(day)+'/'+String(month)+'/'+String(year);
  var concepto = orderid;
  var gateway = data.gateway;
  
  diario.insertRowBefore(2); 
  var info = [fecha,'shopify',month,date,'Tipologia Ingresos',ingresos,0.21,
              ingresos*(1-1/1.21),'Nikarit',concepto,'',
              '705001 Venta de Manteca de Karite,  572003 Banco Caja de Ingenieros',
             ]
  
  for(var i=0; i<info.length;i++){
      diario.getRange(2, i+1).setValue(info[i]);
  }
  diario.getRange(2, 19).setValue(gateway);

  
}


function insertqueryDataTable(data_bq) {
  // Replace this value with the project ID listed in the Google
  // Cloud Platform project.
  var projectId = 'nikaritcloud';
  var dataSetId = 'shopify_clients';
  var tableId = 'main_info';
  
  //var cols="(Row,fecha	total_price	name	sex	email	mantecas	cremas	balsamos	poblacion	latitude	longitude)"

  var fullTableName = "`"+projectId + '.' + dataSetId + '.' + tableId+"`";

  var queryRequest = BigQuery.newQueryRequest();
  
  var data = data_bq;
  queryRequest.query = '#standardSQL\nINSERT ' + fullTableName +'VALUES'+data+';';
  //queryRequest.query ='#standardSQL\nDELETE' + fullTableName + 'WHERE name = "HOLA"'
  var query = BigQuery.Jobs.query(queryRequest, projectId);
  
}


function readId(orderid) {

  // Replace this value with the project ID listed in the Google
  // Cloud Platform project.
 
  var projectId = 'nikaritcloud';
  var dataSetId = 'shopify_clients';
  var tableId = 'main_info';
  
  //var cols="(Row,fecha	total_price	name	sex	email	mantecas	cremas	balsamos	poblacion	latitude	longitude)"

  var fullTableName = "["+projectId + ':' + dataSetId + '.' + tableId+"]";

  var request = {
    query: 'SELECT id ' +
      'FROM '+fullTableName
  };
  var queryResults = BigQuery.Jobs.query(request, projectId);
  var jobId = queryResults.jobReference.jobId;

  // Check on status of the Query Job.
  var sleepTimeMs = 500;
  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId);
  }

  // Get all the rows of results.
  var rows = queryResults.rows;
  while (queryResults.pageToken) {
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
      pageToken: queryResults.pageToken
    });
    rows = rows.concat(queryResults.rows);
  }
  for(var i=0;i<rows.length;i++){
    if(rows[i].f[0].v == orderid){
      Logger.log(rows[i].f[0].v);
      Logger.log('true')
      return true
    }
  }
  // double check
   var ssheet = SpreadsheetApp.openById('ID');
  var todos = ssheet.getSheetByName('Todos');
  var last_row = todos.getLastRow();
  var ids = todos.getSheetValues(2, 14, last_row-2, 1);
  for(var i=0;i<ids.length;i++){
    if(orderid == ids[i]){
      return true
    }
  }
  Logger.log('false')
  return false

}

function deletee() {
  // Replace this value with the project ID listed in the Google
  // Cloud Platform project.
  var projectId = 'nikaritcloud';
  var dataSetId = 'shopify_clients';
  var tableId = 'main_info';
  
  //var cols="(Row,fecha	total_price	name	sex	email	mantecas	cremas	balsamos	poblacion	latitude	longitude)"

  var fullTableName = "`"+projectId + '.' + dataSetId + '.' + tableId+"`";

  var queryRequest = BigQuery.newQueryRequest();
  
  queryRequest.query ="#standardSQL\nDELETE" + fullTableName + "WHERE id = #ID# AND email=''"
  var query = BigQuery.Jobs.query(queryRequest, projectId);
  var jobId = queryResults.jobReference.jobId;
  
}


function count_pedidos(email) {
  // Replace this value with the project ID listed in the Google
  // Cloud Platform project.
  var projectId = 'nikaritcloud';
  var dataSetId = 'shopify_clients';
  var tableId = 'main_info';
  
  //var cols="(Row,fecha	total_price	name	sex	email	mantecas	cremas	balsamos	poblacion	latitude	longitude)"

  var fullTableName = "["+projectId + ':' + dataSetId + '.' + tableId+"]";

  var queryRequest = BigQuery.newQueryRequest();
  var request = {
    query: 'SELECT COUNT(*)\n' +
    'FROM '+fullTableName+
    '\nWHERE email = "'+email+'"'
  };
  var queryResults = BigQuery.Jobs.query(request, projectId);
  var jobId = queryResults.jobReference.jobId;

  // Check on status of the Query Job.
  var sleepTimeMs = 500;
  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId);
  }
  
  return queryResults.rows[0].f[0].v
  
}
function sex_cal(name) {
  var name = name.replace(/ /g, "");
  name = name.toLowerCase();
  name = removeacc(name);
  var url = 'https://raw.githubusercontent.com/marcboquet/spanish-names/master/mujeres.csv';
  var file = UrlFetchApp.fetch(url); // get feed
  var csv = file.getBlob().getDataAsString();
  var csvDatawomen = CSVToArray(csv); 
  
  var url = 'https://raw.githubusercontent.com/marcboquet/spanish-names/master/hombres.csv';
  var file = UrlFetchApp.fetch(url); // get feed
  var csv = file.getBlob().getDataAsString();
  var csvDatamen = CSVToArray(csv); 
  
  for(var i=0;i<csvDatawomen.length;i++){
    var women = String(csvDatawomen[i][0]).toLowerCase()
    if(women == name){
      Logger.log('F');
      Logger.log(women);
      return 'F'
    }
  }
  
  for(var i=0;i<csvDatamen.length;i++){
    var men = String(csvDatamen[i][0]).toLowerCase()
    if(men == name){
      Logger.log('M');
      Logger.log(men);
      return 'M'
    }
  }
  Logger.log('N/A');
  Logger.log(men);
  return 'N/A';

 
};
 
 
// http://www.bennadel.com/blog/1504-Ask-Ben-Parsing-CSV-Strings-With-Javascript-Exec-Regular-Expression-Command.htm
// This will parse a delimited string into an array of
// arrays. The default delimiter is the comma, but this
// can be overriden in the second argument.
 
function CSVToArray(strData, strDelimiter) {
    // Check to see if the delimiter is defined. If not,
    // then default to COMMA.
    strDelimiter = (strDelimiter || ",");
 
    // Create a regular expression to parse the CSV values.
    var objPattern = new RegExp(
        (
            // Delimiters.
            "(\\" + strDelimiter + "|\\r?\\n|\\r|^)" +
 
            // Quoted fields.
            "(?:\"([^\"]*(?:\"\"[^\"]*)*)\"|" +
 
            // Standard fields.
            "([^\"\\" + strDelimiter + "\\r\\n]*))"
        ),
        "gi"
    );
 
    // Create an array to hold our data. Give the array
    // a default empty first row.
    var arrData = [
        []
    ];
 
    // Create an array to hold our individual pattern
    // matching groups.
    var arrMatches = null;
 
    // Keep looping over the regular expression matches
    // until we can no longer find a match.
    while (arrMatches = objPattern.exec(strData)) {
 
        // Get the delimiter that was found.
        var strMatchedDelimiter = arrMatches[1];
 
        // Check to see if the given delimiter has a length
        // (is not the start of string) and if it matches
        // field delimiter. If id does not, then we know
        // that this delimiter is a row delimiter.
        if (
            strMatchedDelimiter.length &&
            (strMatchedDelimiter != strDelimiter)
        ) {
 
            // Since we have reached a new row of data,
            // add an empty row to our data array.
            arrData.push([]);
 
        }
 
        // Now that we have our delimiter out of the way,
        // let's check to see which kind of value we
        // captured (quoted or unquoted).
        if (arrMatches[2]) {
 
            // We found a quoted value. When we capture
            // this value, unescape any double quotes.
            var strMatchedValue = arrMatches[2].replace(
                new RegExp("\"\"", "g"),
                "\""
            );
 
        } else {
 
            // We found a non-quoted value.
            var strMatchedValue = arrMatches[3];
 
        }
 
        // Now that we have our value string, let's add
        // it to the data array.
        arrData[arrData.length - 1].push(strMatchedValue);
    }
 
    // Return the parsed data.
    return (arrData);
};


function removeacc(str){
  var defaultDiacriticsRemovalMap = [
    {'base':'A', 'letters':/[\u0041\u24B6\uFF21\u00C0\u00C1\u00C2\u1EA6\u1EA4\u1EAA\u1EA8\u00C3\u0100\u0102\u1EB0\u1EAE\u1EB4\u1EB2\u0226\u01E0\u00C4\u01DE\u1EA2\u00C5\u01FA\u01CD\u0200\u0202\u1EA0\u1EAC\u1EB6\u1E00\u0104\u023A\u2C6F]/g},
    {'base':'AA','letters':/[\uA732]/g},
    {'base':'AE','letters':/[\u00C6\u01FC\u01E2]/g},
    {'base':'AO','letters':/[\uA734]/g},
    {'base':'AU','letters':/[\uA736]/g},
    {'base':'AV','letters':/[\uA738\uA73A]/g},
    {'base':'AY','letters':/[\uA73C]/g},
    {'base':'B', 'letters':/[\u0042\u24B7\uFF22\u1E02\u1E04\u1E06\u0243\u0182\u0181]/g},
    {'base':'C', 'letters':/[\u0043\u24B8\uFF23\u0106\u0108\u010A\u010C\u00C7\u1E08\u0187\u023B\uA73E]/g},
    {'base':'D', 'letters':/[\u0044\u24B9\uFF24\u1E0A\u010E\u1E0C\u1E10\u1E12\u1E0E\u0110\u018B\u018A\u0189\uA779]/g},
    {'base':'DZ','letters':/[\u01F1\u01C4]/g},
    {'base':'Dz','letters':/[\u01F2\u01C5]/g},
    {'base':'E', 'letters':/[\u0045\u24BA\uFF25\u00C8\u00C9\u00CA\u1EC0\u1EBE\u1EC4\u1EC2\u1EBC\u0112\u1E14\u1E16\u0114\u0116\u00CB\u1EBA\u011A\u0204\u0206\u1EB8\u1EC6\u0228\u1E1C\u0118\u1E18\u1E1A\u0190\u018E]/g},
    {'base':'F', 'letters':/[\u0046\u24BB\uFF26\u1E1E\u0191\uA77B]/g},
    {'base':'G', 'letters':/[\u0047\u24BC\uFF27\u01F4\u011C\u1E20\u011E\u0120\u01E6\u0122\u01E4\u0193\uA7A0\uA77D\uA77E]/g},
    {'base':'H', 'letters':/[\u0048\u24BD\uFF28\u0124\u1E22\u1E26\u021E\u1E24\u1E28\u1E2A\u0126\u2C67\u2C75\uA78D]/g},
    {'base':'I', 'letters':/[\u0049\u24BE\uFF29\u00CC\u00CD\u00CE\u0128\u012A\u012C\u0130\u00CF\u1E2E\u1EC8\u01CF\u0208\u020A\u1ECA\u012E\u1E2C\u0197]/g},
    {'base':'J', 'letters':/[\u004A\u24BF\uFF2A\u0134\u0248]/g},
    {'base':'K', 'letters':/[\u004B\u24C0\uFF2B\u1E30\u01E8\u1E32\u0136\u1E34\u0198\u2C69\uA740\uA742\uA744\uA7A2]/g},
    {'base':'L', 'letters':/[\u004C\u24C1\uFF2C\u013F\u0139\u013D\u1E36\u1E38\u013B\u1E3C\u1E3A\u0141\u023D\u2C62\u2C60\uA748\uA746\uA780]/g},
    {'base':'LJ','letters':/[\u01C7]/g},
    {'base':'Lj','letters':/[\u01C8]/g},
    {'base':'M', 'letters':/[\u004D\u24C2\uFF2D\u1E3E\u1E40\u1E42\u2C6E\u019C]/g},
    {'base':'N', 'letters':/[\u004E\u24C3\uFF2E\u01F8\u0143\u00D1\u1E44\u0147\u1E46\u0145\u1E4A\u1E48\u0220\u019D\uA790\uA7A4]/g},
    {'base':'NJ','letters':/[\u01CA]/g},
    {'base':'Nj','letters':/[\u01CB]/g},
    {'base':'O', 'letters':/[\u004F\u24C4\uFF2F\u00D2\u00D3\u00D4\u1ED2\u1ED0\u1ED6\u1ED4\u00D5\u1E4C\u022C\u1E4E\u014C\u1E50\u1E52\u014E\u022E\u0230\u00D6\u022A\u1ECE\u0150\u01D1\u020C\u020E\u01A0\u1EDC\u1EDA\u1EE0\u1EDE\u1EE2\u1ECC\u1ED8\u01EA\u01EC\u00D8\u01FE\u0186\u019F\uA74A\uA74C]/g},
    {'base':'OI','letters':/[\u01A2]/g},
    {'base':'OO','letters':/[\uA74E]/g},
    {'base':'OU','letters':/[\u0222]/g},
    {'base':'P', 'letters':/[\u0050\u24C5\uFF30\u1E54\u1E56\u01A4\u2C63\uA750\uA752\uA754]/g},
    {'base':'Q', 'letters':/[\u0051\u24C6\uFF31\uA756\uA758\u024A]/g},
    {'base':'R', 'letters':/[\u0052\u24C7\uFF32\u0154\u1E58\u0158\u0210\u0212\u1E5A\u1E5C\u0156\u1E5E\u024C\u2C64\uA75A\uA7A6\uA782]/g},
    {'base':'S', 'letters':/[\u0053\u24C8\uFF33\u1E9E\u015A\u1E64\u015C\u1E60\u0160\u1E66\u1E62\u1E68\u0218\u015E\u2C7E\uA7A8\uA784]/g},
    {'base':'T', 'letters':/[\u0054\u24C9\uFF34\u1E6A\u0164\u1E6C\u021A\u0162\u1E70\u1E6E\u0166\u01AC\u01AE\u023E\uA786]/g},
    {'base':'TZ','letters':/[\uA728]/g},
    {'base':'U', 'letters':/[\u0055\u24CA\uFF35\u00D9\u00DA\u00DB\u0168\u1E78\u016A\u1E7A\u016C\u00DC\u01DB\u01D7\u01D5\u01D9\u1EE6\u016E\u0170\u01D3\u0214\u0216\u01AF\u1EEA\u1EE8\u1EEE\u1EEC\u1EF0\u1EE4\u1E72\u0172\u1E76\u1E74\u0244]/g},
    {'base':'V', 'letters':/[\u0056\u24CB\uFF36\u1E7C\u1E7E\u01B2\uA75E\u0245]/g},
    {'base':'VY','letters':/[\uA760]/g},
    {'base':'W', 'letters':/[\u0057\u24CC\uFF37\u1E80\u1E82\u0174\u1E86\u1E84\u1E88\u2C72]/g},
    {'base':'X', 'letters':/[\u0058\u24CD\uFF38\u1E8A\u1E8C]/g},
    {'base':'Y', 'letters':/[\u0059\u24CE\uFF39\u1EF2\u00DD\u0176\u1EF8\u0232\u1E8E\u0178\u1EF6\u1EF4\u01B3\u024E\u1EFE]/g},
    {'base':'Z', 'letters':/[\u005A\u24CF\uFF3A\u0179\u1E90\u017B\u017D\u1E92\u1E94\u01B5\u0224\u2C7F\u2C6B\uA762]/g},
    {'base':'a', 'letters':/[\u0061\u24D0\uFF41\u1E9A\u00E0\u00E1\u00E2\u1EA7\u1EA5\u1EAB\u1EA9\u00E3\u0101\u0103\u1EB1\u1EAF\u1EB5\u1EB3\u0227\u01E1\u00E4\u01DF\u1EA3\u00E5\u01FB\u01CE\u0201\u0203\u1EA1\u1EAD\u1EB7\u1E01\u0105\u2C65\u0250]/g},
    {'base':'aa','letters':/[\uA733]/g},
    {'base':'ae','letters':/[\u00E6\u01FD\u01E3]/g},
    {'base':'ao','letters':/[\uA735]/g},
    {'base':'au','letters':/[\uA737]/g},
    {'base':'av','letters':/[\uA739\uA73B]/g},
    {'base':'ay','letters':/[\uA73D]/g},
    {'base':'b', 'letters':/[\u0062\u24D1\uFF42\u1E03\u1E05\u1E07\u0180\u0183\u0253]/g},
    {'base':'c', 'letters':/[\u0063\u24D2\uFF43\u0107\u0109\u010B\u010D\u00E7\u1E09\u0188\u023C\uA73F\u2184]/g},
    {'base':'d', 'letters':/[\u0064\u24D3\uFF44\u1E0B\u010F\u1E0D\u1E11\u1E13\u1E0F\u0111\u018C\u0256\u0257\uA77A]/g},
    {'base':'dz','letters':/[\u01F3\u01C6]/g},
    {'base':'e', 'letters':/[\u0065\u24D4\uFF45\u00E8\u00E9\u00EA\u1EC1\u1EBF\u1EC5\u1EC3\u1EBD\u0113\u1E15\u1E17\u0115\u0117\u00EB\u1EBB\u011B\u0205\u0207\u1EB9\u1EC7\u0229\u1E1D\u0119\u1E19\u1E1B\u0247\u025B\u01DD]/g},
    {'base':'f', 'letters':/[\u0066\u24D5\uFF46\u1E1F\u0192\uA77C]/g},
    {'base':'g', 'letters':/[\u0067\u24D6\uFF47\u01F5\u011D\u1E21\u011F\u0121\u01E7\u0123\u01E5\u0260\uA7A1\u1D79\uA77F]/g},
    {'base':'h', 'letters':/[\u0068\u24D7\uFF48\u0125\u1E23\u1E27\u021F\u1E25\u1E29\u1E2B\u1E96\u0127\u2C68\u2C76\u0265]/g},
    {'base':'hv','letters':/[\u0195]/g},
    {'base':'i', 'letters':/[\u0069\u24D8\uFF49\u00EC\u00ED\u00EE\u0129\u012B\u012D\u00EF\u1E2F\u1EC9\u01D0\u0209\u020B\u1ECB\u012F\u1E2D\u0268\u0131]/g},
    {'base':'j', 'letters':/[\u006A\u24D9\uFF4A\u0135\u01F0\u0249]/g},
    {'base':'k', 'letters':/[\u006B\u24DA\uFF4B\u1E31\u01E9\u1E33\u0137\u1E35\u0199\u2C6A\uA741\uA743\uA745\uA7A3]/g},
    {'base':'l', 'letters':/[\u006C\u24DB\uFF4C\u0140\u013A\u013E\u1E37\u1E39\u013C\u1E3D\u1E3B\u017F\u0142\u019A\u026B\u2C61\uA749\uA781\uA747]/g},
    {'base':'lj','letters':/[\u01C9]/g},
    {'base':'m', 'letters':/[\u006D\u24DC\uFF4D\u1E3F\u1E41\u1E43\u0271\u026F]/g},
    {'base':'n', 'letters':/[\u006E\u24DD\uFF4E\u01F9\u0144\u00F1\u1E45\u0148\u1E47\u0146\u1E4B\u1E49\u019E\u0272\u0149\uA791\uA7A5]/g},
    {'base':'nj','letters':/[\u01CC]/g},
    {'base':'o', 'letters':/[\u006F\u24DE\uFF4F\u00F2\u00F3\u00F4\u1ED3\u1ED1\u1ED7\u1ED5\u00F5\u1E4D\u022D\u1E4F\u014D\u1E51\u1E53\u014F\u022F\u0231\u00F6\u022B\u1ECF\u0151\u01D2\u020D\u020F\u01A1\u1EDD\u1EDB\u1EE1\u1EDF\u1EE3\u1ECD\u1ED9\u01EB\u01ED\u00F8\u01FF\u0254\uA74B\uA74D\u0275]/g},
    {'base':'oi','letters':/[\u01A3]/g},
    {'base':'ou','letters':/[\u0223]/g},
    {'base':'oo','letters':/[\uA74F]/g},
    {'base':'p','letters':/[\u0070\u24DF\uFF50\u1E55\u1E57\u01A5\u1D7D\uA751\uA753\uA755]/g},
    {'base':'q','letters':/[\u0071\u24E0\uFF51\u024B\uA757\uA759]/g},
    {'base':'r','letters':/[\u0072\u24E1\uFF52\u0155\u1E59\u0159\u0211\u0213\u1E5B\u1E5D\u0157\u1E5F\u024D\u027D\uA75B\uA7A7\uA783]/g},
    {'base':'s','letters':/[\u0073\u24E2\uFF53\u00DF\u015B\u1E65\u015D\u1E61\u0161\u1E67\u1E63\u1E69\u0219\u015F\u023F\uA7A9\uA785\u1E9B]/g},
    {'base':'t','letters':/[\u0074\u24E3\uFF54\u1E6B\u1E97\u0165\u1E6D\u021B\u0163\u1E71\u1E6F\u0167\u01AD\u0288\u2C66\uA787]/g},
    {'base':'tz','letters':/[\uA729]/g},
    {'base':'u','letters':/[\u0075\u24E4\uFF55\u00F9\u00FA\u00FB\u0169\u1E79\u016B\u1E7B\u016D\u00FC\u01DC\u01D8\u01D6\u01DA\u1EE7\u016F\u0171\u01D4\u0215\u0217\u01B0\u1EEB\u1EE9\u1EEF\u1EED\u1EF1\u1EE5\u1E73\u0173\u1E77\u1E75\u0289]/g},
    {'base':'v','letters':/[\u0076\u24E5\uFF56\u1E7D\u1E7F\u028B\uA75F\u028C]/g},
    {'base':'vy','letters':/[\uA761]/g},
    {'base':'w','letters':/[\u0077\u24E6\uFF57\u1E81\u1E83\u0175\u1E87\u1E85\u1E98\u1E89\u2C73]/g},
    {'base':'x','letters':/[\u0078\u24E7\uFF58\u1E8B\u1E8D]/g},
    {'base':'y','letters':/[\u0079\u24E8\uFF59\u1EF3\u00FD\u0177\u1EF9\u0233\u1E8F\u00FF\u1EF7\u1E99\u1EF5\u01B4\u024F\u1EFF]/g},
    {'base':'z','letters':/[\u007A\u24E9\uFF5A\u017A\u1E91\u017C\u017E\u1E93\u1E95\u01B6\u0225\u0240\u2C6C\uA763]/g}
];
  var changes = defaultDiacriticsRemovalMap;
  for(var i=0; i<changes.length; i++) {
    str = str.replace(changes[i].letters, changes[i].base);
  }
  return str;
}
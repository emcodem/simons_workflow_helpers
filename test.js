// newfor protocol as documented in https://evs.com/documentation/synapse/Content/GNS600/Appendix%202%20NewFor%20Protocol.htm?TocPath=Resources%7CGNS600%7C_____2
// ported from https://github.com/peterkvt80/newfor-subtitles/blob/master/newfor.pde

const net = require('net');

var client = {};
// Helper: log Buffer as hex bytes and printable ASCII
function logBufferHexAscii(buf, label) {
  try {
    const bytes = Array.from(buf);
    const hex = bytes.map(b => b.toString(16).padStart(2, '0')).join(' ');
    const ascii = bytes.map(b => (b >= 32 && b <= 126) ? String.fromCharCode(b) : '.').join('');
    console.debug(label + ' hex: ' + hex);
    console.debug(label + ' ascii: ' + ascii);
  } catch (e) {
    // fallback
    try { console.debug(label, buf.toString('hex')); } catch (ex) { console.debug(label, buf); }
  }
}
var HamTab = [0x15,0x02,0x49,0x5E,0x64,0x73,0x38,0x2F,0xD0,0xC7,0x8C,0x9B,0xA1,0xB6,0xFD,0xEA]; //hamming parity encode
var titleIsActive = false;
var m_previousClearTimer;

async function newForSetText(host,port,stringArray,durationms = 2.5){
  //stringArray=maximum 3 title rows, if duration is not set, we also send reveal and clear commands automatically
  //if durationSec is not set, you must send clear and reveal on your own
  
  //BUILD / Set text
  //Sends subtitle information to inserter or receiving device.
  //Packet length variable depending on the number of subtitle rows.
  if (!client.writable){
    client = await internalSocketConnect (host,port);
    await sleep(40);
    writeTeletextPage(host,port);
	return;
    await sleep(40);
    writeTeletextPage2(host,port);
  }

  if (stringArray.length > 3)
    throw new Error("can only send 3 rows at a time");
  for (var i = 0;i<stringArray.length;i++){
    if (stringArray[i].length > 35)
      throw new Error("Each row can only be 35 characters: " + stringArray[i]);
    //pad short strings with whitespaces
    if (stringArray[i].length < 35)
      stringArray[i] = padSpaces(stringArray[i],35)

    stringArray[i] = "\r\v\v" + stringArray[i] + "\n\n"; //\r makes all lines bigger on FAB display
  }

  

  var rowcount = stringArray.length;
 
  //the following data byte does not make sense but it is how FAB works
  await sleep(40);
  newForClear(host,port);
  if (m_previousClearTimer != null) {
    clearTimeout(m_previousClearTimer); 
    m_previousClearTimer = null;
  }

  //calculate rows buffer
  var finalTitleBuffer = Buffer.from("");
  
  for (var i = 0;i<stringArray.length;i++){ //each subtitle row
    //encode to odd parity
    var _rb = makeOddParity(Buffer.from(stringArray[i],"latin1"));
    //prepend rowNumSelector, 2 bytes row number 	Range 01-23, Hamming encoded into 2 bytes, high nibble in the first byte
    var topPos = false;
    finalTitleBuffer = Buffer.concat([finalTitleBuffer,rowNumSelector(i,stringArray.length,topPos),_rb]);
  }

    //ClearBit and RowCount, ham encoded
    var clearBitAndRowCount = 0xC7;
    if (rowcount == 2)
      clearBitAndRowCount = 0x8C;  
    if (rowcount == 3)
      clearBitAndRowCount = 0x9B;

  //message starts with prefix 0x8F and clearbit/rowcount (e.g. 09=ham C7 for 1 row)
  var prefix = Buffer.concat([bfi(0x8F),bfi(clearBitAndRowCount)]);
  finalTitleBuffer = Buffer.concat([prefix,finalTitleBuffer]);

  logBufferHexAscii(finalTitleBuffer, "Sending Subtitle Buffer");
  console.debug("Text: ",stringArray);
  await sleep(40);
  client.write(finalTitleBuffer);
  //can receive 0x86 as return but we don't care

  //if duration is not set, do not show and clear the sub.
  if (!durationms)
    return
  
  await sleep(40);
  newForReveal();
  try{
    durationms = parseFloat(durationms)
  }catch(ex){
    console.error("Could not parse duration to float")
  }

  
  m_previousClearTimer = setTimeout(async function(){
                      try{
                          await newForClear(host,port);
                      }catch(ex){
                        console.error(ex)
                      }
                      },durationms);

}

async function newForReveal(host,port){
  if (!client.writable)
    client = await internalSocketConnect (host,port);
  logBufferHexAscii(bfi(0x10), "Sending Reveal");
  //client.write(bfi(0x86));
  client.write(bfi(0x10));
}

async function newForClear(host,port){
  if (!client.writable)
    client = await internalSocketConnect (host,port);

  logBufferHexAscii(bfi(0x98), "Sending Clear");
  client.write(bfi(0x98));
}

void sendPageNumber(page)
{
  
  client.write(0x0e); // Page Init command 
  client.write(HamTab[0]); // Always 0
  client.write(HamTab[page/0x100]); // H
  client.write(HamTab[(page&0xf0)/0x10]); // T
  client.write(HamTab[page&0x0f]); // U
}


async function writeTeletextPage(host,port){
  //CONNECT
  //Establishes a connection from the originator to the destination inserter or receiving device, 
  //ahead of transferring one or more subtitles for transmission. The command packet must contain valid Teletext magazine (range 1-8) and Teletext page (range 0-99).
  //Total packet length = 5 bytes
  let byteArray = [bfi(0x0e),bfi(HamTab[0]),bfi(HamTab[8]),bfi(HamTab[8]),bfi(HamTab[8])];
  logBufferHexAscii(Buffer.concat(byteArray), "Sending Teletext Page 888");
  client.write(Buffer.concat([bfi(0x0e),bfi(HamTab[0]),bfi(HamTab[8]),bfi(HamTab[8]),bfi(HamTab[8])])); // U   
  
}
async function writeTeletextPage2(host,port){
  //CONNECT
  //Establishes a connection from the originator to the destination inserter or receiving device, 
  //ahead of transferring one or more subtitles for transmission. The command packet must contain valid Teletext magazine (range 1-8) and Teletext page (range 0-99).
  //Total packet length = 5 bytes
  client.write(Buffer.concat([bfi(0x0e),bfi(HamTab[0]),bfi(HamTab[0]),bfi(HamTab[0]),bfi(HamTab[1])])); // U   
 
}

async function internalSocketConnect(host,port){
  /* returns socket client, use client.write(Buffer.from([byteToSend])); */
  return new Promise((resolve, reject) => {
    try{
      client = new net.Socket();
      client.connect(port, host, () => {
          console.debug('Connected to the server');
          
          //internal_newForConnect();
          if (client.writable) {
            resolve(client);
          } else {
            reject(new Error('Connection is not writable'));
          }
        })
        client.on('data', (data) => {
          console.log('Received data:', data.toString());
        });
        client.on('close', () => {
          console.log('Connection closed');
        });
        client.on('error', (err) => {
          console.error('Connection error:', err);
          reject(err);
        });
    }catch(ex){
      reject(ex);
    }
  });//promise
}

//helpers

function padSpaces(str,maxlen = 36){
	//places string in the middle of maxlen character line
	var strlen = str.length;
	if (strlen == maxlen)
		return
	var numspaces = maxlen - strlen;
	var leftspaces = Math.ceil(numspaces / 2);
	str = str.padStart(leftspaces + strlen)
	str = str.padEnd(maxlen)
	return str;
}

function rowNumSelector(currentRow,rowCount,topPos = false){
  var high_nybble = 0x02;
  if (topPos)
    high_nybble = 0x15;
	var linePlacing = [
	  // Row High nybble //alwas 0x01 (ham encoded: 0x02) for low subs, 0x00 (0x15 ham) for high subs
	  // Row Low nybble // first line is 0x06 for 1 line (bottom), 0x04 for 2(middle), 0x02 for 2 lines (highest) (ham encoded: 0x38,0x64,0x49)
	  Buffer.concat([bfi(high_nybble),bfi(0x38)]), //bottom position (ham encoded)
	  Buffer.concat([bfi(high_nybble),bfi(0x64)]), //middle pos
	  Buffer.concat([bfi(high_nybble),bfi(0x49)]), //highes pos
    //[0x15,0x02,0x49,0x5E,0x64,0x73,0x38,0x2F,0xD0,0xC7,0x8C,0x9B,0xA1,0xB6,0xFD,0xEA];
	]
	if (rowCount == 1){
	  return linePlacing[0];
	}
	if (rowCount == 2){
	  switch (currentRow) {
		case 0:
		  return linePlacing[1];
		case 1:
		  return linePlacing[0];
		default:
		  throw new Error("Cannot select rownum, currentRow: ",currentRow,"rowCount",rowCount);
	  }
	}
	if (rowCount == 3){
	  switch (currentRow) {
		case 0:
		  return linePlacing[2];
		case 1:
		  return linePlacing[1];            
		case 2:
		  return linePlacing[0];
		default:
		  throw new Error("Cannot select rownum, currentRow: ",currentRow,"rowCount",rowCount);
	  }
	}
}

function makeOddParity(_all){
	/* ensures all characters in Buffer are odd parity (use ascii string buffer) */
	for(var i = 0;i<_all.length;i++){
		var n = _all[i];
    n = charMap(n);
		if (!isOdd(n)){
			var mask = 1 << 7; // gets the 8th bit
			n |= mask;
			
		}
    _all[i] = n;
	}
	return _all;
}

function charMap(n){
  //ETSI EN 300 706 german charmap https://de.wikipedia.org/wiki/Teletext-Zeichens%C3%A4tze_(ETSI_EN_300_706)
  
  var map = {
    //before we map ETSI, let's translate the overwritten chars to something similar
    0x40:0x2a,0x60:0x27, //@ to *
    0x5b:0x28,0x5c:0x2F,0x5d:0x29, //[\] to (/)
    0x7b:0x28,0x7c:0x49,0x7d:0x29,0x7e:0x2d, //{|}~ to (I)-
    0xb0:0xba, //° to º
    //we must also map everything >= 0x80 to something meaningful as we only have 7bit chars in teletext
    0xc0:0xc1,0xc3:0xc1,0xc5:0xc1,0xc6:0xc4,//all forms of A to A

    //finally apply the standard ETSI EN 300 706 Deutsch öäüÖÄÜ to []{} etc...
    0xa7:0x40,0xc4:0x5b,0xd6:0x5c,0xdc:0x5d,0xb0:0x60,0xe4:0x7b,0xf6:0x7c,0xfc:0x7d,0xdf:0x7e, 
  }
  if (map[n]){
    console.log("translated from",n, "to",map[n])
    return map[n]
  }
  return n;
}

function isOdd(n)
{
	/* checks if sum of set bits in a byte is odd*/
	var count = 0;
	while (n)
	 {
	   count += n & 1;
	   n >>= 1;
	 }
	 return count%2==1;
}
	


function bfi(int){
  /* buffer from int, single number to single byte */
  var buf = Buffer.alloc(1);
  buf.writeUInt8(int, 0);
  return buf;  
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
//exports

module.exports = {
  newForSetText:newForSetText,
  newForReveal:newForReveal,
  newForClear:newForClear,
}

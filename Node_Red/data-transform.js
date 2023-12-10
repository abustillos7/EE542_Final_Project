// console.log(msg.payload);
var parts = msg.payload.split(",");
// F,rh,ppm,ppmc
var temp = parseFloat(parts[0]);
var humidity = parseFloat(parts[1]);
var ppm = parseFloat(parts[2]);
var ppmc = parseFloat(parts[3].split(";")[0]);

// var num = parseInt(msg.payload)
// if(isNaN(num)){
//     num = -1;
// } else {
//     num = num/100;
// }
// console.log(num)

var now = new Date().getTime()
var data = {
    "ts": now,
    // "value": {
    "ppm":ppm,
    "humidity":humidity,
    "temp":temp,
    "ppmc": ppmc
    
    // },
}
// var data = {
//     "ts": now,
//     "value": 
//     num
//     // {
//     //     "thermister": num
//     // }
// }

msg.payload = JSON.stringify(data);
return msg;
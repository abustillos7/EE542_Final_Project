// {"ts":1667705639811,"ppm":494.596222,"humidity":35,"temp":71.599998,"ppmc":445.847168}

var data = JSON.parse(msg.payload)
data['sample_time'] = data['ts']
data['device_id'] = '1'

delete data['ts']
//msg.payload = JSON.stringify(data);
msg.payload = data;
return msg;
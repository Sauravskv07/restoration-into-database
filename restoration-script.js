const fs=require('fs');
const path=require('path');
const mongodb=require('mongodb');
const filename1='m3-customer-address-data.json';
const async=require('async');
const obj1=JSON.parse(fs.readFileSync(path.join(__dirname, filename1), { encoding : 'utf8'}));
const filename2='m3-customer-data.json';
const obj2=JSON.parse(fs.readFileSync(path.join(__dirname, filename2), { encoding : 'utf8'}));
//console.log(obj1);


const sizeOfPacket=parseInt(process.argv[2]);
const numberOfTask=parseInt(obj1.length/sizeOfPacket);
//console.log(sizeOfPacket);
//console.log(numberOfTask);

const taskArray=[];
const url='mongodb://localhost:27017/BitCoin';
mongodb.MongoClient.connect(url,{ useNewUrlParser: true },(error,client)=>
{	
	if(error) 
	{
		console.log('there has been an error while connecting');
		return process.exit(1);
	}
	console.log('connected to the mongodb database system');
	const db=client.db('Customers');
	var collection=db.collection('Restored-Customers');
	//console.log(collection);
	var taskNo=0;
	var i=0;
	for(taskNo=0;taskNo<numberOfTask;taskNo++)
	{	//console.log('making of task  '+taskNo);
		taskArray[taskNo]=()=>
		{
			//console.log('the system is executing some tasks');
			var parseNo=0;
			for(parseNo=0;parseNo<sizeOfPacket;parseNo++)
			{
				//console.log('making a query');
				i++;
				//console.log(i);
				if(i==obj1.length)
				break;
				//console.log('making an object');
				var objnewhelper=Object.assign({},obj1[i]);
				var objnew=Object.assign(obj2[i],objnewhelper);
				//console.log(objnew);
				collection.insertOne(objnew,(error,result)=>
				{
					if(error)
					{
						console.log('hey there has been an error');
						process.exit(1);
					}
					//console.log('hey something is entered in the database');
				});		
			}
		};
		//console.log(taskArray[taskNo]);
	}
	async.parallel(taskArray,(error,results)=>
	{
	if(error) 
		{
			console.log('there has been an error while doing some task');
			return process.exit(1);
		}
	});

	client.close();
});


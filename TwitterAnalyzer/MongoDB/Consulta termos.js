db.users.find({'_id': ObjectId('5b140a46f949bc0af0b9e07b')})

db.termos.find({$and:[{termo:'lula'},{entidades:{$size:1}}]})

db.termos.aggregate([
  {$match:{'termo':'lula'}},
  {$unwind:'$entidades'},
  {$match:{'entidades':'Dilma'}},
  {
    $group:{
      	  _id:{termo:'$termo',entidade:'$entidades',sentimento:'$sentimento'},
           count:{$sum:1}
  	}
  }
])
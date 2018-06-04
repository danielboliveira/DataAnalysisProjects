db.users.aggregate([
  {$match:{'user_verified':false}},
  {
        $group  : {
           _id  : { age: '$user_age'},
            avg : { $avg: '$user_statuses_avg' },
           count: { $sum: 1 }
        }
   },
   {
     $project:
     {
       _id:'$_id',
       avg:'$avg',
       count:'$count',
     }
   }
  ])
  
 db.users.find({user_age:1}) 
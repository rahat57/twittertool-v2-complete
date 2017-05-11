 <?php
 ini_set('max_execution_time', 0); //0=NOLIMIT
 if ( !isset( $_SERVER['REMOTE_ADDR'] ) ) {
  $_SERVER['REMOTE_ADDR'] = '127.0.0.1';
}
/**
 * Root directory of Drupal installation.
 */
define('DRUPAL_ROOT', '/var/www/twitter-tool-v2-dev.xululabs.us/htdocs');

require_once DRUPAL_ROOT . '/includes/bootstrap.inc';
drupal_bootstrap(DRUPAL_BOOTSTRAP_FULL);
require "Logging.php";

/**
* post curl request
*@param url
*@param params
*return output
*/
//$users = entity_load('user');
$req_array = array($users);
	// if(!empty($user->field_consumer_key) && !empty($user->field_consumer_secret) && !empty($user->field_access_token) && !empty($user->field_access_token_secret)) {

$role = user_role_load_by_name('influencer');
  $query = 'SELECT ur.uid
    FROM {users_roles} AS ur
    WHERE ur.rid = :rid';
  $result = db_query($query, array(':rid' => $role->rid));
  $uids = $result->fetchCol();
  $res=user_load_multiple($uids);
  $reel=json_decode(json_encode($res),true);

  foreach ($reel as $key ) {
 //echo print_r($key['uid'],true);
   $request= array( 
     'credentials'=> json_encode(
       array(
         "consumerKey"=>$key['field_consumer_key']['und'][0]['value'],
         "consumerSecret"=>$key['field_consumer_secret']['und'][0]['value'],
         "accessToken"=>$key['field_access_token']['und'][0]['value'],
         "accessTokenSecret"=>$key['field_access_token_secret']['und'][0]['value']
       )
     )
   );
print "<pre>".print_r($request,true)."<pre>";
httpPostUser("http://localhost:8182/indexUser",$request);
//geting screen name of current credentials
$request1= array( 
     'credentials'=> json_encode(
       array(
          "consumerKey"=>$key['field_consumer_key']['und'][0]['value'],
         "consumerSecret"=>$key['field_consumer_secret']['und'][0]['value'],
         "accessToken"=>$key['field_access_token']['und'][0]['value'],
         "accessTokenSecret"=>$key['field_access_token_secret']['und'][0]['value']
       )
     )
   );

  $res=httpPostUser("http://localhost:8182/userInfo",$request1);
  $dett=json_decode($res,true);
  //getting whole data of current credentials by screenNAme
  $request2= array( 
     'keyword'=>$dett['userInfo'][0]['userScreenName'],
     'searchIn'=>json_encode(array('userScreenName')),
       );

$res2=httpPostUser("http://localhost:8181/search",$request2);
$det=json_decode($res2,true);
//print "<pre>".print_r($det,true)."</pre>";
echo print_r($det['documents']['0']['unfollowedFollowers'],true);
//if($det['documents']['0']['unfollowedFollowers'] != null){
 foreach ($det['documents'][0]['unfollowedFollowers'] as $fet) 
 { print "<pre>".print_r($fet,true)."</pre>";
echo "kchhh ghjkghjklghjlghj------------------------".$fet;
  $requesti= array( 
     'keyword'=>$fet,
     'searchIn'=>json_encode(array('id')),
       );

   $resi=httpPostUser("http://localhost:8181/searchUserRelation",$requesti);
   $deti=json_decode($resi,true);
   //print "<pre>".print_r($deti['documents'][0]['id'],true)."<pre>";
$query =  db_select('unfollow_table','lt')
    ->fields('lt')
    ->condition('uid', $user->uid,'=')
    ->condition('scid',$deti['documents'][0]['id'],'=')
    ->execute();
    $countAll = $query->rowCount();
    if($countAll==0){
db_insert('unfollow_table')
    ->fields(array(
      'uid' => $key['uid'],
      'scid' => $deti['documents'][0]['id'],
      'date'=>  $deti['documents'][0]['date']
    ))->execute();
  print "<pre>".print_r("Inserted",true)."<pre>";
}
else
{} 


 }
//  // }//if 
}


function httpPostUser($url,$params)
{
  $postData = '';
   //create name value pairs seperated by &
  foreach($params as $k => $v) 
  { 
    $postData .= $k . '='.$v.'&'; 
  }
  $postData = rtrim($postData, '&');
  $ch = curl_init();  

  curl_setopt($ch,CURLOPT_URL,$url);
  curl_setopt($ch,CURLOPT_RETURNTRANSFER,true);
  curl_setopt($ch,CURLOPT_HEADER, false); 
  curl_setopt($ch, CURLOPT_POST, count($postData));
  curl_setopt($ch, CURLOPT_POSTFIELDS, $postData);    

  $output=curl_exec($ch);
print "<pre>".print_r($output, true)."</pre>";
  curl_close($ch);
  return $output;

}
?>
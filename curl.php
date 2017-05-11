<?php
function httpPost($url,$params)
{
  $postData = '';
   //create name value pairs seperated by &
  foreach($params as $k => $v) 
  { 
    $postData .= $k . '='.$v.'&'; 
	echo "loop values ".postData;

  }
  $postData = rtrim($postData, '&');

  $ch = curl_init();  

  curl_setopt($ch,CURLOPT_URL,$url);
  curl_setopt($ch,CURLOPT_RETURNTRANSFER,true);
  curl_setopt($ch,CURLOPT_HEADER, false); 
  curl_setopt($ch, CURLOPT_POST, count($postData));
  curl_setopt($ch, CURLOPT_POSTFIELDS, $postData);    

  $output=curl_exec($ch);

  curl_close($ch);
  return $output;

}

$paramsRetweets = array(
 'credentials'=>json_encode(array("consumerKey"=>"OaWkShcQdw8sNunYHz1RnOUcw","consumerSecret"=>"YebsFfE08CW1lUT2t0aJ8THoMMcbgMcJIhE8fZgYG4pW1HFCKp","accessToken"=>"799593858585739264-0TeruB6ZPknwxnwJvGSox409eY9Hv2w","accessTokenSecret"=>"Rxw5ROeh0vbA528r6OyIfxDjTHuMpqffRX9z7LxDJoPqk"))
 );

echo httpPost("http://localhost:8182/indexUser",$paramsRetweets);
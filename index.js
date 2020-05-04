exports.handler = async (event,response) => {
    // TODO implement
    const password ="serverless";
    var user_pass = event.password;

    if(password==user_pass){
      response="<a  target=\"_blank\" href=\"http://d3buj5pmx58xr1.cloudfront.net/2020-05-03%2020-32-03.mp4\" >Click Here</a> to watch video"
    }else {
        response="wrong password"

    }
    return response;
};

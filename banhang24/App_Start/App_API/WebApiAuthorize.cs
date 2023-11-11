using System.Collections.Generic;
using System.Linq;
using System.Web.Http.Controllers;

namespace banhang24.App_Start.App_API
{
    public class WebApiAuthorize : System.Web.Http.AuthorizeAttribute
    {
        public override void OnAuthorization(HttpActionContext actionContext)
        {
            //base.OnAuthorization(actionContext);
            IEnumerable<string> values;
            if (actionContext.Request.Headers.TryGetValues("ApiKey", out values))
            {
                string apikey = values.First();
            }
            string actionName = actionContext.ActionDescriptor.ActionName;
            base.IsAuthorized(actionContext);
        }
    }
}
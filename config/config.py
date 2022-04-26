from datetime import timedelta

##1# Setup the Flask-JWT-Extended extension
JWT_COOKIE_SECURE = False
JWT_TOKEN_LOCATION = ["cookies"]
JWT_SECRET_KEY = "equipetoolingmasdev"
PROPAGATE_EXCEPTIONS = True
JWT_ACCESS_TOKEN_EXPIRES = timedelta(days=30)
#development
#production
#test
#! / bin / bash 

exec sudo systemctl daemon-reload 
exec sudo systemctl enable produce
exec systemctl status produce.service

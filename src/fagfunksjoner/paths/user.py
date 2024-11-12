# +
import os, subprocess, getpass

def find_email() -> str:
    """Find the users email from the environment.
    
    Returns:
        str: Hopefully the users email.
    
    Raises:
        ValueError: If we cant find any sources of the users email.
    """
    # Try to get from set variable
    dapla_mail = verify_ssbmail(os.environ.get("DAPLA_USER", None))
    if dapla_mail:
        return dapla_mail
    # Try to find in Jupyter-env var
    jup_user = verify_ssbmail(os.environ.get("JUPYTERHUB_USER", None))
    if jup_user:
        return jup_user
    # Maybe the user used their ssb-mail in git?
    git_email = verify_ssbmail(subprocess.run(['git', 'config', 'user.email'], capture_output=True, text=True).stdout.strip())
    if git_email:
        return git_email
    # Last hail-mary...
    getpass_user = verify_ssbmail(getpass.getuser())
    if getpass_user:
        return getpass_user
   
    raise ValueError("Cant find the users email or tbf in the system.")

def find_user() -> str:
    """Find the user shortname in the environment.
    
    Returns:
        str: Hopefully the users three-character username.
    """
    return find_email().split("@")[0]

def verify_ssbmail(user: str | None) -> str | None:
    """Verify and modify user into user email.
    
    Args:
        user: The username to verify or change into a mail.
    
    Returns:
        str | None: If verifys correctly, returns a string, that should be a full mail.
            If not correct, returns None.
    """
    if user is None:
        return None
    if "@" in user and len(user.split("@")[0]) == 3:
        return user
    if "@" not in user and len(user) == 3:
        return user + "@ssb.no"
# -



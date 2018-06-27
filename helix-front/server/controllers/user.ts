import { Request, Response, Router } from 'express';

import * as request from 'request';
import * as LdapClient from 'ldapjs';

import { LDAP, CheckAdmin } from '../config';

export class UserCtrl {

  constructor(router: Router) {
    router.route('/user/authorize').get(this.authorize);
    router.route('/user/login').post(this.login.bind(this));
    router.route('/user/current').get(this.current);
    router.route('/user/can').get(this.can);
  }

  // please rewrite this function to support your own authorization logic
  protected authorize(req: Request, res: Response) {
    if (req.query.name) {
      // since it's bypass mode, skip admin check
      req.session.username = req.query.name;

      if (req.query.url) {
        res.redirect(req.query.url);
      } else {
        res.redirect('/');
      }
    } else {
      res.status(401).send('Unauthorized');
    }
  }

  protected current(req: Request, res: Response) {
    res.json(req.session.username || 'Guest');
  }

  protected can(req: Request, res: Response) {
    res.json(req.session.isAdmin ? true : false);
  }

  protected login(req: Request, res: Response) {
    const credential = req.body;
    if (!credential.username || !credential.password) {
      res.status(401).json(false);
      return;
    }

    // check LDAP
    const ldap = LdapClient.createClient({ url: LDAP.uri });
    ldap.bind(credential.username + LDAP.principalSuffix, credential.password, err => {
      if (err) {
        res.status(401).json(false);
      } else {
        // authroized
        req.session.username = credential.username;
        CheckAdmin(req.session.username, (isAdmin: boolean) => {
          req.session.isAdmin = isAdmin;
          res.json(true);
        });
      }
    });
  }

}

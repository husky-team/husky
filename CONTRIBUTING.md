About Contribution
=====

### Before contributing
A contribution may be related to bug fixing, feature enhancement, or application addition.  
Please make sure to create a **new** issue (double-check if your issue has not been discussed already) on [issues tracker](https://github.com/husky-team/husky/issues) first.  
Then, follow the steps below. All issues related to improving Husky are very much appreciated, and could be discussed with the Husky Team.

### Fork

1. Fork Husky as your own repository.
2. Clone your own Husky to local: `$ git clone YOUR/Husky.git`
3. Add upstream remote to your local repository: `$ git remote add upstream https://github.com/husky-team/husky.git`
4. Fetch upstream: `$ git fetch upstream`


### Pull Request

1. Sync to upstream: `$ git checkout master && git rebase upstream/master`
2. Create your developing branch: `$ git checkout -b dev`
3. Make your changes and commit them: `$ git add FILES && git commit -m "MESSAGE"`
4. Follow this format for the commit message:

    \[PART\] (1) Commit summary(2)  
    Issues(3)  
    Detailed description(4)

5. Before committing, it would be better to pass `lint.py` and `clang-format.py`: `$ cd husky && HUSKY_ROOT=. ./scripts/lint.py && HUSKY_ROOT=. ./scripts/clang-format.py -o check`. More usage of the scripts:
```
$ ./scripts/lint.py [FILE or DIR]
$ ./scripts/clang-format.py -o [replace, check]
$ ./scripts/clang-format.py -o [replace, check] [FILE or DIR]
```
6. Push local commit to your own repository: `$ git push -u/f origin dev`
7. Create a pull request to the upstream repository. Then you can `@reviewers` to review your patch.

More about the commit message:

    (1) [PART] shows what section of Husky your patch is addressed to. If the patch is addressed to a very specific subsection, this can be represented as [Part][SubPart].  
    (2) Commit summary needs to tell WHAT IS BEING DONE. DON'T overextend it. Capitalize the first letter. It is not necessary to add a period.
    (3) If there are any issues on the issue tracker that are being dealt with by this patch, they can be listed here.
    (4) Add necessary descriptions about this patch such as usages, plans or technologies. Use punctuation.


For example:

```
[Build][InputFormat] Fix error for xxx not found
issue #xx
Should add macros before including xxx.
```

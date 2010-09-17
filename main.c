#include "ruby.h"
#include "jlog.h"
#include "fcntl.h"

typedef struct {
   jlog_ctx *ctx;
   char *path;
   jlog_id start;
   jlog_id last;
   jlog_id prev;
   jlog_id end;
   int auto_checkpoint;
   int error;
} jlog_obj;

typedef jlog_obj * JLog;
typedef jlog_obj * JLog_Writer;
typedef jlog_obj * JLog_Reader;

VALUE cJLog;
VALUE cJLogWriter;
VALUE cJLogReader;
VALUE eJLog;

void rJLog_populate_subscribers(VALUE);

void rJLog_free(JLog jo) {
   if(jo->ctx) {
      jlog_ctx_close(jo->ctx);
   }

   if(jo->path) {
      free(jo->path);
   }

   free(jo);
}

void rJLog_raise(JLog jo, char* mess)
{
   VALUE e = rb_exc_new2(eJLog, mess);
   rb_iv_set(e, "error", INT2FIX(jlog_ctx_err(jo->ctx)));
   rb_iv_set(e, "errstr", rb_str_new2(jlog_ctx_err_string(jo->ctx)));
   rb_iv_set(e, "errno", INT2FIX(jlog_ctx_errno(jo->ctx)));

   rb_exc_raise(e);
}

VALUE rJLog_new(int argc, VALUE* argv, VALUE klass) {
   VALUE rjlog;
   size_t size = 0;
   int options = O_CREAT;
   JLog jo = ALLOC(jlog_obj);
   char *path = STR2CSTR(argv[0]);

   jo->ctx = jlog_new(path);
   jo->path = strdup(path);
   jo->auto_checkpoint = 0;

   if(argc < 1) {
      rb_raise(rb_eArgError, "at least one argument is required (the path)");
   }

   if(argc > 2) {
      options = FIX2INT(argv[2]);
      if(argc > 3) {
         size = NUM2INT(argv[3]);
      }
   }
   
   if(!jo->ctx) {
      rJLog_free(jo);
      rb_raise(eJLog, "jlog_new(%s) failed", path);
   }

   if(options & O_CREAT) {
      if(jlog_ctx_init(jo->ctx) != 0) {
         if(jlog_ctx_err(jo->ctx) == JLOG_ERR_CREATE_EXISTS) {
            if(options & O_EXCL) {
               rJLog_free(jo);
               rb_raise(eJLog, "file already exists: %s", path);
            }
         }else {
            rJLog_raise(jo, "Error initializing jlog");
         }
      }
      jlog_ctx_close(jo->ctx);
      jo->ctx = jlog_new(path);
      if(!jo->ctx) {
         rJLog_free(jo);
         rb_raise(eJLog, "jlog_new(%s) failed after successful init", path);
      }
   }

   rjlog = Data_Wrap_Struct(klass, 0, rJLog_free, jo);
   rb_obj_call_init(rjlog, argc, argv);

   return rjlog;
}

static VALUE rJLog_initialize(int argc, VALUE* argv, VALUE klass)
{
   JLog jo;

   Data_Get_Struct(klass, jlog_obj, jo);

   rJLog_populate_subscribers(klass);

   return klass;
}

VALUE rJLog_add_subscriber(int argc, VALUE* argv, VALUE self)
{
   char *subscriber;
   long whence;
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   subscriber = (NIL_P(argv[0]) ? NULL : StringValuePtr(argv[0]));
   whence = (NIL_P(argv[1]) ? 0 : NUM2LONG(argv[1]));

   if(!jo || !jo->ctx || jlog_ctx_add_subscriber(jo->ctx, subscriber, whence) != 0) {
      return Qfalse;
   }

   rJLog_populate_subscribers(self);

   return Qtrue;
}


VALUE rJLog_remove_subscriber(VALUE self, VALUE subscriber)
{
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx || jlog_ctx_remove_subscriber(jo->ctx, STR2CSTR(subscriber)) != 0)
   {
      //rb_raise(eJLog, "FAILED");
      return subscriber;
   }

   rJLog_populate_subscribers(self);

   return Qtrue;
}

void rJLog_populate_subscribers(VALUE self)
{
   char **list;
   int i;
   JLog jo;
   VALUE subscribers = rb_ary_new();

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx)
   {
      rb_raise(eJLog, "Invalid jlog context");
   }

   jlog_ctx_list_subscribers(jo->ctx, &list);
   for(i=0; list[i]; i++ ) {
      rb_ary_push(subscribers, rb_str_new2(list[i]));
   }
   jlog_ctx_list_subscribers_dispose(jo->ctx, list);

   rb_iv_set(self, "@subscribers", subscribers);
}

VALUE rJLog_list_subscribers(VALUE self)
{
   JLog jo;
   VALUE subscribers;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx)
   {
      rb_raise(eJLog, "Invalid jlog context");
   }

   subscribers = rb_iv_get(self, "@subscribers");


   return subscribers;
}

VALUE rJLog_raw_size(VALUE self)
{
   size_t size;
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) {
      rb_raise(eJLog, "Invalid jlog context");
   }
   size = jlog_raw_size(jo->ctx);

   return INT2NUM(size);
}

VALUE rJLog_close(VALUE self)
{
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) return Qnil;

   jlog_ctx_close(jo->ctx);
   jo->ctx = NULL;

   return Qtrue;
}

VALUE rJLog_inspect(VALUE self)
{
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) return Qnil;

   // XXX Fill in inspect call data 

   return Qtrue;
}

VALUE rJLog_destroy(VALUE self)
{
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo) return Qnil;

   rJLog_free(jo);

   return Qtrue;
}

VALUE rJLog_W_open(VALUE self)
{
   JLog_Writer jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) {
      rb_raise(eJLog, "Invalid jlog context");
   }

   if(jlog_ctx_open_writer(jo->ctx) != 0) {
      rJLog_raise(jo, "jlog_ctx_open_writer failed");
   }

   return Qtrue;
}

VALUE rJLog_W_write(int argc, VALUE *argv, VALUE self)
{
   char *buffer;
   int ts = 0;
   jlog_message m;
   struct timeval t;
   long buffer_len;
   JLog_Writer jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) {
      rb_raise(eJLog, "Invalid jlog context");
   }

   if(argc > 3) {
      ts = (time_t) NUM2INT(argv[3]);
   }

   buffer = rb_str2cstr(argv[0], &buffer_len);
   m.mess = buffer;
   m.mess_len = buffer_len;
   t.tv_sec = ts;
   t.tv_usec = 0;

   if(jlog_ctx_write_message(jo->ctx, &m, ts ? &t : NULL) < 0) {
      return Qfalse;
   } else {
      return Qtrue;
   }
}


VALUE rJLog_R_open(VALUE self, VALUE subscriber)
{
   JLog_Reader jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) {
      rb_raise(eJLog, "Invalid jlog context");
   }

   if(jlog_ctx_open_reader(jo->ctx, STR2CSTR(subscriber)) != 0) {
      rJLog_raise(jo, "jlog_ctx_open_reader failed");
   }

   return Qtrue;
}


VALUE rJLog_R_read(VALUE self)
{
   const jlog_id epoch = {0, 0};
   jlog_id cur;
   jlog_message message;
   int cnt;
   JLog_Reader jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) {
      rb_raise(eJLog, "Invalid jlog context");
   }

   // If start is unset, read the interval
   if(jo->error || !memcmp(&jo->start, &epoch, sizeof(jlog_id)))
   {
      jo->error = 0;
      cnt = jlog_ctx_read_interval(jo->ctx, &jo->start, &jo->end);
      if(cnt == 0 || (cnt == -1 && jlog_ctx_err(jo->ctx) == JLOG_ERR_FILE_OPEN)) {
         jo->start = epoch;
         jo->end = epoch;
         return Qnil;
      }
      else if(cnt == -1)
         rJLog_raise(jo, "jlog_ctx_read_interval_failed");
   }

   // If last is unset, start at the beginning
   if(!memcmp(&jo->last, &epoch, sizeof(jlog_id))) {
      cur = jo->start;
   } else {
      // if we've already read the end, return; otherwise advance
      cur = jo->last;
      if(!memcmp(&jo->prev, &jo->end, sizeof(jlog_id))) {
         jo->start = epoch;
         jo->end = epoch;
         return Qnil;
      }
      jlog_ctx_advance_id(jo->ctx, &jo->last, &cur, &jo->end);
      if(!memcmp(&jo->last, &cur, sizeof(jlog_id))) {
            jo->start = epoch;
            jo->end = epoch;
            return Qnil;
      }
   }
   if(jlog_ctx_read_message(jo->ctx, &cur, &message) != 0) {
      if(jlog_ctx_err(jo->ctx) == JLOG_ERR_FILE_OPEN) {
         jo->error = 1;
         return Qnil;
      }

      // read failed; raise error but recover if read is retried
      jo->error = 1;
      rJLog_raise(jo, "read failed");
   }
   if(jo->auto_checkpoint) {
      if(jlog_ctx_read_checkpoint(jo->ctx, &cur) != 0)
         rJLog_raise(jo, "checkpoint failed");

      // must reread the interval after a checkpoint
      jo->last = epoch;
      jo->prev = epoch;
      jo->start = epoch;
      jo->end = epoch;
   } else {
      // update last
      jo->prev = jo->last;
      jo->last = cur;
   }

   return rb_str_new2(message.mess);
   /* XXX
    * ruby_array = [message.mess, message.mess_len]
    * return ruby_array
   return Qnil;
    */
}


VALUE rJLog_R_rewind(VALUE self)
{
   JLog_Reader jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) {
      rb_raise(eJLog, "Invalid jlog context");
   }

   jo->last = jo->prev;

   return Qtrue;
}


VALUE rJLog_R_checkpoint(VALUE self)
{
   jlog_id epoch = { 0, 0 };
   JLog_Reader jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) {
      rb_raise(eJLog, "Invalid jlog context");
   }

   if(memcmp(&jo->last, &epoch, sizeof(jlog_id)))
   {
      jlog_ctx_read_checkpoint(jo->ctx, &jo->last);

      // re-read the interval
      jo->last = epoch;
      jo->start = epoch;
      jo->end = epoch;
   }

   return Qtrue;
}


VALUE rJLog_R_auto_checkpoint(int argc, VALUE *argv, VALUE self)
{
   JLog jo;

   Data_Get_Struct(self, jlog_obj, jo);

   if(!jo || !jo->ctx) {
      rb_raise(eJLog, "Invalid jlog context");
   }

   if(argc > 0) {
      int ac = FIX2INT(argv[0]);
      jo->auto_checkpoint = ac;
   }

   return INT2FIX(jo->auto_checkpoint);
}


void Init_jlog(void) {
   cJLog = rb_define_class("JLog", rb_cObject);
   cJLogWriter = rb_define_class_under(cJLog, "Writer", cJLog);
   cJLogReader = rb_define_class_under(cJLog, "Reader", cJLog);

   eJLog = rb_define_class_under(cJLog, "Error", rb_eStandardError);

   rb_define_singleton_method(cJLog, "new", rJLog_new, -1);
   rb_define_method(cJLog, "initialize", rJLog_initialize, -1);

   rb_define_method(cJLog, "add_subscriber", rJLog_add_subscriber, -1);
   rb_define_method(cJLog, "remove_subscriber", rJLog_remove_subscriber, 1);
   rb_define_method(cJLog, "list_subscribers", rJLog_list_subscribers, 0);
   rb_define_method(cJLog, "raw_size", rJLog_raw_size, 0);
   rb_define_method(cJLog, "close", rJLog_close, 0);

   rb_define_method(cJLog, "destroy", rJLog_destroy, 0);
   rb_define_method(cJLog, "inspect", rJLog_inspect, 0);

   rb_define_alias(cJLog, "size", "raw_size");

   rb_define_singleton_method(cJLogWriter, "new", rJLog_new, -1);
   rb_define_method(cJLogWriter, "initialize", rJLog_initialize, -1);
   rb_define_method(cJLogWriter, "open", rJLog_W_open, 0);
   rb_define_method(cJLogWriter, "write", rJLog_W_write, -1);

   rb_define_singleton_method(cJLogReader, "new", rJLog_new, -1);
   rb_define_method(cJLogReader, "initialize", rJLog_initialize, -1);
   rb_define_method(cJLogReader, "open", rJLog_R_open, 1);
   rb_define_method(cJLogReader, "read", rJLog_R_read, 0);
   rb_define_method(cJLogReader, "rewind", rJLog_R_rewind, 0);
   rb_define_method(cJLogReader, "checkpoint", rJLog_R_checkpoint, 0);
   rb_define_method(cJLogReader, "auto_checkpoint", rJLog_R_auto_checkpoint, -1);
}


/* ErrorType message, p 
 * message "; error: %d (%s) errno: %d (%s)",
 * jlog_ctx_err(p->ctx), jlog_ctx_err_string(p->ctx), jlog_ctx_errno(p->ctx), strerror(jlog_ctx_errno(j->ctx))
*/


import tensorflow as tf

# Here is the first resnet layer, that does preserve input shape
# ResNET Layer
class ResidualLayer(tf.keras.layers.Layer):
  def __init__(self, f=None, fillter_size_top=None,
               fillter_size_mid=None, fillter_size_bot=None):
    super(ResidualLayer, self).__init__()
    self.conv_top_1 = tf.keras.layers.Conv1D(fillter_size_top, 1,
                                             strides=1, padding='valid')
    # Divided by 2 to ensure different parameters
    self.conv_top_2 = tf.keras.layers.Conv1D(fillter_size_top//4, 1,
                                             strides=1, padding='valid')
    self.conv_mid_1 = tf.keras.layers.Conv1D(fillter_size_mid, f,
                                             strides=1, padding='same')
    # Divided by 2 to ensure different parameters
    self.conv_mid_2 = tf.keras.layers.Conv1D(fillter_size_mid//4, f,
                                             strides=1, padding='same')
    self.conv_bot_1 = tf.keras.layers.Conv1D(fillter_size_bot, 1,
                                             strides=1, padding='valid')
    # The outputs have to be the same to add up
    self.conv_bot_2 = tf.keras.layers.Conv1D(fillter_size_bot, 1,
                                             strides=1, padding='valid')
    
    self.batch_norm_top_1 = tf.keras.layers.BatchNormalization(axis=2)
    self.batch_norm_top_2 = tf.keras.layers.BatchNormalization(axis=2)
    self.batch_norm_mid_1 = tf.keras.layers.BatchNormalization(axis=2)
    self.batch_norm_mid_2 = tf.keras.layers.BatchNormalization(axis=2)
    self.batch_norm_bot_1 = tf.keras.layers.BatchNormalization(axis=2)
    self.batch_norm_bot_2 = tf.keras.layers.BatchNormalization(axis=2)

    self.activation_relu = tf.keras.layers.Activation('relu')
    self.add_op = tf.keras.layers.Add()

  def call(self, input_x, training=False):
    x_shortcut = input_x

    ##PATH 1
    x_path_1 = input_x
    # First CONV block of path 1
    x_path_1 = self.conv_top_1(x_path_1)
    x_path_1 = self.batch_norm_top_1(x_path_1, training=training)
    x_path_1 = self.activation_relu(x_path_1)

    # Second CONV block of path 1
    x_path_1 = self.conv_mid_1(x_path_1)
    x_path_1 = self.batch_norm_mid_1(x_path_1, training=training)
    x_path_1 = self.activation_relu(x_path_1)

    # Third CONV block of path 1
    x_path_1 = self.conv_bot_1(x_path_1)
    x_path_1 = self.batch_norm_bot_1(x_path_1, training=training)

    ##PATH 2
    x_path_2 = input_x
    # First CONV block of path 1
    x_path_2 = self.conv_top_2(x_path_2)
    x_path_2 = self.batch_norm_top_2(x_path_2, training=training)
    x_path_2 = self.activation_relu(x_path_2)

    # Second CONV block of path 1
    x_path_2 = self.conv_mid_2(x_path_2)
    x_path_2 = self.batch_norm_mid_2(x_path_2, training=training)
    x_path_2 = self.activation_relu(x_path_2)

    # Third CONV block of path 1
    x_path_2 = self.conv_bot_2(x_path_2)
    x_path_2 = self.batch_norm_bot_2(x_path_2, training=training)

    # Addition of PATH 1 and PATH 2
    x = self.add_op([x_path_1, x_path_2])
    # Addition to the shortcut path
    x = self.add_op([x, x_shortcut])
    x_output = self.activation_relu(x)

    return x_output

# Here is the second resnet layer, that does not preserve input shape
# ResNET layer
class ResidualLayerScal(tf.keras.layers.Layer):
  def __init__(self, f=None, s=None, fillter_size_top=None,
               fillter_size_mid=None, fillter_size_bot=None):
    super(ResidualLayerScal, self).__init__()
    self.conv_top_1 = tf.keras.layers.Conv1D(fillter_size_top, 1,
                                             strides=1, padding='valid')
    # Make the hyperparameters different 
    self.conv_top_2 = tf.keras.layers.Conv1D(fillter_size_top//4, 1,
                                             strides=1, padding='valid')
    self.conv_mid_1 = tf.keras.layers.Conv1D(fillter_size_mid, f,
                                             strides=1, padding='same')
    # Make the hyperparameters different 
    self.conv_mid_2 = tf.keras.layers.Conv1D(fillter_size_mid//4, f,
                                             strides=1, padding='same')
    self.conv_bot_1 = tf.keras.layers.Conv1D(fillter_size_bot, 1,
                                             strides=s, padding='valid')
    # You can't make the hyperparameters different, the output have to be the same
    self.conv_bot_2 = tf.keras.layers.Conv1D(fillter_size_bot, 1,
                                             strides=s, padding='valid')
    self.conv_scal = tf.keras.layers.Conv1D(fillter_size_bot, 1,
                                             strides=s, padding='valid')
    
    self.batch_norm_top_1 = tf.keras.layers.BatchNormalization(axis=2)
    self.batch_norm_top_2 = tf.keras.layers.BatchNormalization(axis=2)
    self.batch_norm_mid_1 = tf.keras.layers.BatchNormalization(axis=2)
    self.batch_norm_mid_2 = tf.keras.layers.BatchNormalization(axis=2)
    self.batch_norm_bot_1 = tf.keras.layers.BatchNormalization(axis=2)
    self.batch_norm_bot_2 = tf.keras.layers.BatchNormalization(axis=2)
    self.batch_norm_scal = tf.keras.layers.BatchNormalization(axis=2)

    self.activation_relu = tf.keras.layers.Activation('relu')
    self.add_op = tf.keras.layers.Add()

  def call(self, input_x, training=False):
    x_shortcut = input_x

    ##PATH 1
    x_path_1 = input_x
    # First CONV block of path 1
    x_path_1 = self.conv_top_1(x_path_1)
    x_path_1 = self.batch_norm_top_1(x_path_1, training=training)
    x_path_1 = self.activation_relu(x_path_1)

    # Second CONV block of path 1
    x_path_1 = self.conv_mid_1(x_path_1)
    x_path_1 = self.batch_norm_mid_1(x_path_1, training=training)
    x_path_1 = self.activation_relu(x_path_1)

    # Third CONV block of path 1
    x_path_1 = self.conv_bot_1(x_path_1)
    x_path_1 = self.batch_norm_bot_1(x_path_1, training=training)

    ##PATH 2
    x_path_2 = input_x
    # First CONV block of path 1
    x_path_2 = self.conv_top_2(x_path_2)
    x_path_2 = self.batch_norm_top_2(x_path_2, training=training)
    x_path_2 = self.activation_relu(x_path_2)

    # Second CONV block of path 1
    x_path_2 = self.conv_mid_2(x_path_2)
    x_path_2 = self.batch_norm_mid_2(x_path_2, training=training)
    x_path_2 = self.activation_relu(x_path_2)

    # Third CONV block of path 1
    x_path_2 = self.conv_bot_2(x_path_2)
    x_path_2 = self.batch_norm_bot_2(x_path_2, training=training)

    # Addition of PATH 1 and PATH 2
    x = self.add_op([x_path_1, x_path_2])
    # Scaling Block
    x_shortcut = self.conv_scal(x_shortcut)
    x_shortcut = self.batch_norm_scal(x_shortcut, training=training)
    # Addition to the shortcut path
    x = self.add_op([x, x_shortcut])
    x_output = self.activation_relu(x)

    return x_output

# Then the layers are combined into a model
class FunnyResNet(tf.keras.Model):
  def __init__(self, num_classes):
    super(FunnyResNet, self).__init__()
    self.batchnorm_lay1 = tf.keras.layers.BatchNormalization(axis=2)
    self.conv_lay1 = tf.keras.layers.Conv1D(64, strides=2, kernel_size=3, activation='relu')
    self.act_lay1 = tf.keras.layers.Activation('relu')


  # Our ResNet block 1
    self.resnet_b1_lay1 = ResidualLayerScal(f=3, s=2, fillter_size_top=128,
                                            fillter_size_mid=128, fillter_size_bot=256)
    self.resnet_b1_lay2 = ResidualLayer(f=3, fillter_size_top=128,
                                        fillter_size_mid=128, fillter_size_bot=256)
    self.resnet_b1_lay3 = ResidualLayer(f=3, fillter_size_top=128,
                                        fillter_size_mid=128, fillter_size_bot=256)
    self.resnet_b1_lay4 = ResidualLayer(f=3, fillter_size_top=128,
                                        fillter_size_mid=128, fillter_size_bot=256)
    self.resnet_b1_lay5 = ResidualLayer(f=3, fillter_size_top=128,
                                        fillter_size_mid=128, fillter_size_bot=256)

    # Our ResNet block 2
    self.resnet_b2_lay1 = ResidualLayerScal(f=5, s=2, fillter_size_top=256,
                                            fillter_size_mid=256, fillter_size_bot=512)
    self.resnet_b2_lay2 = ResidualLayer(f=5, fillter_size_top=256,
                                        fillter_size_mid=256, fillter_size_bot=512)
    self.resnet_b2_lay3 = ResidualLayer(f=5, fillter_size_top=256,
                                        fillter_size_mid=256, fillter_size_bot=512)
    self.resnet_b2_lay4 = ResidualLayer(f=5, fillter_size_top=256,
                                        fillter_size_mid=256, fillter_size_bot=512)
    self.resnet_b2_lay5 = ResidualLayer(f=5, fillter_size_top=256,
                                        fillter_size_mid=256, fillter_size_bot=512)

  # Our ResNet block 3
    self.resnet_b3_lay1 = ResidualLayerScal(f=7, s=2, fillter_size_top=512,
                                            fillter_size_mid=512, fillter_size_bot=1024)
    self.resnet_b3_lay2 = ResidualLayer(f=7, fillter_size_top=512,
                                        fillter_size_mid=512, fillter_size_bot=1024)
    self.resnet_b3_lay3 = ResidualLayer(f=7, fillter_size_top=512,
                                        fillter_size_mid=512, fillter_size_bot=1024)
    self.resnet_b3_lay4 = ResidualLayer(f=7, fillter_size_top=512,
                                        fillter_size_mid=512, fillter_size_bot=1024)
    self.resnet_b3_lay5 = ResidualLayer(f=7, fillter_size_top=512,
                                        fillter_size_mid=512, fillter_size_bot=1024)


    self.flat_lay = tf.keras.layers.Flatten()
    self.dense_softmax = tf.keras.layers.Dense(num_classes, activation='softmax')

  def call(self, x_input, training=False, transfer=False):
    x = self.conv_lay1(x_input)
    x = self.batchnorm_lay1(x, training=training)
    x = self.act_lay1(x)
    # Residual block 1
    x = self.resnet_b1_lay1(x, training=training)
    x = self.resnet_b1_lay2(x, training=training)
    x = self.resnet_b1_lay3(x, training=training)
    x = self.resnet_b1_lay4(x, training=training)
    x = self.resnet_b1_lay5(x, training=training)
    # Residual block 2
    x = self.resnet_b2_lay1(x, training=training)
    x = self.resnet_b2_lay2(x, training=training)
    x = self.resnet_b2_lay3(x, training=training)
    x = self.resnet_b2_lay4(x, training=training)
    x = self.resnet_b2_lay5(x, training=training)
    # Residual block 3
    x = self.resnet_b3_lay1(x, training=training)
    x = self.resnet_b3_lay2(x, training=training)
    x = self.resnet_b3_lay3(x, training=training)
    x = self.resnet_b3_lay4(x, training=training)
    x = self.resnet_b3_lay5(x, training=training)

    x = self.flat_lay(x)
    #Softmax
    training = training or transfer
    x = self.dense_softmax(x, training=retrain)
    return x